#!/bin/bash
# =====================================================
# Kafka + Zookeeper + Kafka Connect Control Script (Dynamic IP Support)
# Ubuntu 22.04 - Kafka 3.9.0
# Safe start/stop/status with dynamic advertised.listeners
# Works for both Bridged and NAT (Port Forwarding) modes
# =====================================================

KAFKA_HOME="/opt/kafka/kafka_2.13-3.9.0"
ZOOKEEPER_CONFIG="$KAFKA_HOME/config/zookeeper.properties"
KAFKA_CONFIG="$KAFKA_HOME/config/server.properties"
CONNECT_CONFIG="$KAFKA_HOME/config/connect-distributed.properties"

ZOOKEEPER_LOG="$KAFKA_HOME/logs/zookeeper.log"
KAFKA_LOG="$KAFKA_HOME/logs/kafka.log"
CONNECT_LOG="$KAFKA_HOME/logs/connect.log"

# ------------------------------
# Helper Functions
# ------------------------------

get_vm_ip() {
  ip addr show | grep "inet " | grep -v "127.0.0.1" | awk '{print $2}' | cut -d'/' -f1 | head -1
}

get_zookeeper_port() {
  grep "^clientPort" "$ZOOKEEPER_CONFIG" | cut -d'=' -f2 | tr -d ' ' || echo "2181"
}

get_kafka_port() {
  PORT=$(grep -E "^listeners=|^advertised.listeners=" "$KAFKA_CONFIG" | head -1 | grep -oP ':\K[0-9]+' | head -1)
  if [ -z "$PORT" ]; then PORT="9092"; fi
  echo "$PORT"
}

get_connect_port() {
  grep "^rest.port" "$CONNECT_CONFIG" | cut -d'=' -f2 | tr -d ' ' || echo "8083"
}

check_port() {
  local port=$1
  if command -v ss &> /dev/null; then
    ss -tulpn 2>/dev/null | grep ":$port " | grep -q LISTEN
  else
    netstat -tulpn 2>/dev/null | grep ":$port " | grep -q LISTEN
  fi
}

# ------------------------------
# Dynamic IP Update for Kafka
# ------------------------------

update_kafka_config() {
  VM_IP=$(get_vm_ip)
  KAFKA_PORT=$(get_kafka_port)

  if [[ "$VM_IP" == 192.168.* || "$VM_IP" == 10.* ]]; then
    ADVERTISED="$VM_IP"
  else
    ADVERTISED="localhost"
  fi

  echo " Updating Kafka configuration..."
  echo " - Detected VM IP: $VM_IP"
  echo " - Advertising as: $ADVERTISED:$KAFKA_PORT"

  sed -i "s|^listeners=.*|listeners=PLAINTEXT://0.0.0.0:$KAFKA_PORT|" "$KAFKA_CONFIG"
  if grep -q "^advertised.listeners=" "$KAFKA_CONFIG"; then
    sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://$ADVERTISED:$KAFKA_PORT|" "$KAFKA_CONFIG"
  else
    echo "advertised.listeners=PLAINTEXT://$ADVERTISED:$KAFKA_PORT" >> "$KAFKA_CONFIG"
  fi
}

# ------------------------------
# Start Services
# ------------------------------

start_services() {
  # --- Start Zookeeper ---
  if pgrep -f "QuorumPeerMain|zookeeper" > /dev/null; then
    echo " Zookeeper is already running."
  else
    echo " Starting Zookeeper..."
    nohup "$KAFKA_HOME/bin/zookeeper-server-start.sh" "$ZOOKEEPER_CONFIG" > "$ZOOKEEPER_LOG" 2>&1 &
    echo $! > /tmp/zookeeper.pid
    sleep 5
    echo " Zookeeper started."
  fi

  ZK_PORT=$(get_zookeeper_port)
  check_port "$ZK_PORT" && echo " Zookeeper listening on port: $ZK_PORT"

  # --- Start Kafka ---
  if pgrep -f "kafka.Kafka" > /dev/null; then
    echo " Kafka is already running."
  else
    update_kafka_config
    echo " Starting Kafka..."
    nohup "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONFIG" > "$KAFKA_LOG" 2>&1 &
    echo $! > /tmp/kafka.pid
    sleep 8
    echo " Kafka started."
  fi

  KAFKA_PORT=$(get_kafka_port)
  check_port "$KAFKA_PORT" && echo " Kafka listening on port: $KAFKA_PORT"

  # --- Start Kafka Connect ---
  if pgrep -f "ConnectDistributed" > /dev/null; then
    echo " Kafka Connect is already running."
  else
    echo " Starting Kafka Connect..."
    nohup "$KAFKA_HOME/bin/connect-distributed.sh" "$CONNECT_CONFIG" > "$CONNECT_LOG" 2>&1 &
    echo $! > /tmp/connect.pid
    sleep 5
    echo " Kafka Connect started."
  fi

  CONNECT_PORT=$(get_connect_port)
  check_port "$CONNECT_PORT" && echo "🔌 Kafka Connect REST API on port: $CONNECT_PORT"

  VM_IP=$(get_vm_ip)
  echo ""
  echo "════════════════════════════════════════════════════════════"
  echo " Service Status Summary:"
  echo " Zookeeper → Port $ZK_PORT"
  echo " Kafka → Port $KAFKA_PORT"
  echo " Kafka Connect → Port $CONNECT_PORT"
  echo ""
  echo " VM IP Address: $VM_IP"
  echo ""
  echo " Bootstrap Server: $VM_IP:$KAFKA_PORT"
  echo " Connect REST API:  http://$VM_IP:$CONNECT_PORT/"
  echo "════════════════════════════════════════════════════════════"
}

# ------------------------------
# Stop Services
# ------------------------------

stop_services() {
  echo " Stopping Kafka Connect..."
  if [ -f /tmp/connect.pid ]; then
    kill -TERM $(cat /tmp/connect.pid) 2>/dev/null
    rm -f /tmp/connect.pid
    echo " Kafka Connect stopped via PID file."
  else
    pkill -f "ConnectDistributed" && echo " Kafka Connect stopped." || echo " Kafka Connect not running."
  fi

  echo " Stopping Kafka..."
  if [ -f /tmp/kafka.pid ]; then
    kill -TERM $(cat /tmp/kafka.pid) 2>/dev/null
    rm -f /tmp/kafka.pid
    echo " Kafka stopped via PID file."
  else
    pkill -f "kafka.Kafka" && echo " Kafka stopped." || echo " Kafka not running."
  fi

  echo " Stopping Zookeeper..."
  if [ -f /tmp/zookeeper.pid ]; then
    kill -TERM $(cat /tmp/zookeeper.pid) 2>/dev/null
    rm -f /tmp/zookeeper.pid
    echo " Zookeeper stopped via PID file."
  else
    pkill -f "QuorumPeerMain|zookeeper" && echo " Zookeeper stopped." || echo " Zookeeper not running."
  fi

  echo " All services stopped gracefully."
}

# ------------------------------
# Status
# ------------------------------

status_services() {
  echo " Checking status..."
  echo ""

  if pgrep -f "kafka.Kafka" > /dev/null; then
    echo " Kafka is running."
  else
    echo " Kafka is not running."
  fi

  if pgrep -f "QuorumPeerMain|zookeeper" > /dev/null; then
    echo " Zookeeper is running."
  else
    echo " Zookeeper is not running."
  fi

  if pgrep -f "ConnectDistributed" > /dev/null; then
    echo " Kafka Connect is running."
  else
    echo " Kafka Connect is not running."
  fi

  echo ""
  echo " Active Ports:"
  if command -v ss &> /dev/null; then
    ss -tulpn 2>/dev/null | grep -E ":(2181|9092|8083) " || echo " No Kafka/Zookeeper/Connect ports found"
  else
    netstat -tulpn 2>/dev/null | grep -E ":(2181|9092|8083) " || echo " No Kafka/Zookeeper/Connect ports found"
  fi
}

# ------------------------------
# Main Command
# ------------------------------

case "$1" in
  start) start_services ;;
  stop) stop_services ;;
  status) status_services ;;
  restart)
    stop_services
    sleep 3
    start_services
    ;;
  *)
    echo "Usage: $0 {start|stop|status|restart}"
    exit 1
    ;;
esac

