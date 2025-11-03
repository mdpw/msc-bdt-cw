#!/bin/bash

# Graceful Start/Stop Script for Hadoop, Spark, and YARN Services
# Usage: ./bigdata_services.sh [start|stop|restart|status]

ACTION=${1:-status}

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if a service is running
check_service() {
    local service_name=$1
    local port=$2
    
    if [ -n "$port" ]; then
        if netstat -ln 2>/dev/null | grep -q ":$port "; then
            echo "✅ Running"
            return 0
        else
            echo "❌ Not running"
            return 1
        fi
    else
        if jps | grep -q "$service_name"; then
            echo "✅ Running"
            return 0
        else
            echo "❌ Not running"
            return 1
        fi
    fi
}

# Function to wait for service to start/stop
wait_for_service() {
    local service_name=$1
    local port=$2
    local action=$3  # start or stop
    local timeout=30
    local count=0
    
    while [ $count -lt $timeout ]; do
        if [ "$action" = "start" ]; then
            if check_service "$service_name" "$port" >/dev/null 2>&1; then
                return 0
            fi
        else
            if ! check_service "$service_name" "$port" >/dev/null 2>&1; then
                return 0
            fi
        fi
        sleep 1
        count=$((count + 1))
    done
    return 1
}

# Function to start services
start_services() {
    print_status "Starting Hadoop and Spark services..."
    
    # Start HDFS
    print_status "Starting HDFS..."
    start-dfs.sh
    if wait_for_service "NameNode" "9870"; then
        print_success "HDFS started successfully"
    else
        print_warning "HDFS may not have started properly"
    fi
    
    # Start YARN
    print_status "Starting YARN..."
    start-yarn.sh
    if wait_for_service "ResourceManager" "8088"; then
        print_success "YARN started successfully"
    else
        print_warning "YARN may not have started properly"
    fi
    
    # Start MapReduce History Server
    print_status "Starting MapReduce History Server..."
    mapred --daemon start historyserver
    if wait_for_service "JobHistoryServer" "19888"; then
        print_success "MapReduce History Server started"
    else
        print_warning "MapReduce History Server may not have started"
    fi
    
    # Start Spark History Server
    print_status "Starting Spark History Server..."
    start-history-server.sh >/dev/null 2>&1
    if wait_for_service "HistoryServer" "18080"; then
        print_success "Spark History Server started"
    else
        print_warning "Spark History Server may not have started"
    fi
    
    print_success "All services startup completed!"
}

# Function to stop services
stop_services() {
    print_status "Stopping Hadoop and Spark services..."
    
    # Stop Spark History Server
    print_status "Stopping Spark History Server..."
    stop-history-server.sh >/dev/null 2>&1
    if wait_for_service "HistoryServer" "18080" "stop"; then
        print_success "Spark History Server stopped"
    else
        print_warning "Spark History Server may still be running"
    fi
    
    # Stop MapReduce History Server
    print_status "Stopping MapReduce History Server..."
    mapred --daemon stop historyserver
    if wait_for_service "JobHistoryServer" "19888" "stop"; then
        print_success "MapReduce History Server stopped"
    else
        print_warning "MapReduce History Server may still be running"
    fi
    
    # Stop YARN
    print_status "Stopping YARN..."
    stop-yarn.sh
    if wait_for_service "ResourceManager" "8088" "stop"; then
        print_success "YARN stopped successfully"
    else
        print_warning "YARN may still be running"
    fi
    
    # Stop HDFS
    print_status "Stopping HDFS..."
    stop-dfs.sh
    if wait_for_service "NameNode" "9870" "stop"; then
        print_success "HDFS stopped successfully"
    else
        print_warning "HDFS may still be running"
    fi
    
    print_success "All services shutdown completed!"
}

# Function to show service status
show_status() {
    echo "=================================================="
    echo "🔍 HADOOP & SPARK SERVICES STATUS"
    echo "=================================================="
    
    echo ""
    echo "📊 HDFS Services:"
    printf "   NameNode (9870):        "
    check_service "NameNode" "9870"
    printf "   DataNode:               "
    check_service "DataNode"
    printf "   SecondaryNameNode:      "
    check_service "SecondaryNameNode"
    
    echo ""
    echo "🧮 YARN Services:"
    printf "   ResourceManager (8088): "
    check_service "ResourceManager" "8088"
    printf "   NodeManager:            "
    check_service "NodeManager"
    
    echo ""
    echo "📚 History Services:"
    printf "   MapReduce History (19888): "
    check_service "JobHistoryServer" "19888"
    printf "   Spark History (18080):     "
    check_service "HistoryServer" "18080"
    
    echo ""
    echo "🌐 Web UIs (if services are running):"
    if check_service "NameNode" "9870" >/dev/null 2>&1; then
        echo "   📊 HDFS: http://localhost:9870"
    fi
    if check_service "ResourceManager" "8088" >/dev/null 2>&1; then
        echo "   🧮 YARN: http://localhost:8088"
    fi
    if check_service "JobHistoryServer" "19888" >/dev/null 2>&1; then
        echo "   📚 MapReduce History: http://localhost:19888"
    fi
    if check_service "HistoryServer" "18080" >/dev/null 2>&1; then
        echo "   📈 Spark History: http://localhost:18080"
    fi
    
    echo ""
    echo "🔧 Running Java Processes:"
    jps | grep -E "(NameNode|DataNode|ResourceManager|NodeManager|JobHistoryServer|HistoryServer)" | sort
    
    echo ""
    echo "=================================================="
}

# Function to restart services
restart_services() {
    print_status "Restarting all services..."
    stop_services
    sleep 3
    start_services
}

# Main script logic
case $ACTION in
    start)
        echo "🚀 Starting Hadoop and Spark Services"
        echo "======================================"
        start_services
        echo ""
        show_status
        ;;
    stop)
        echo "🛑 Stopping Hadoop and Spark Services"
        echo "======================================"
        stop_services
        echo ""
        show_status
        ;;
    restart)
        echo "🔄 Restarting Hadoop and Spark Services"
        echo "========================================"
        restart_services
        echo ""
        show_status
        ;;
    status)
        show_status
        ;;
    *)
        echo "Usage: $0 [start|stop|restart|status]"
        echo ""
        echo "Commands:"
        echo "  start   - Start all Hadoop and Spark services"
        echo "  stop    - Stop all Hadoop and Spark services"
        echo "  restart - Restart all services"
        echo "  status  - Show current service status (default)"
        echo ""
        echo "Examples:"
        echo "  $0 start"
        echo "  $0 stop"
        echo "  $0 restart"
        echo "  $0 status"
        exit 1
        ;;
esac