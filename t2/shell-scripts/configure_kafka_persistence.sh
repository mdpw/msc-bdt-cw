#!/bin/bash
# =====================================================
# Simplified Kafka Data Persistence Configuration
# Focuses on persistent storage with manual backup/restore
# =====================================================

KAFKA_HOME="/opt/kafka/kafka_2.13-3.9.0"
KAFKA_CONFIG="$KAFKA_HOME/config/server.properties"
ZOOKEEPER_CONFIG="$KAFKA_HOME/config/zookeeper.properties"

# Persistent data directories (outside of /tmp)
KAFKA_DATA_DIR="/var/kafka-data"
ZOOKEEPER_DATA_DIR="/var/zookeeper-data"

# Manual backup directory
BACKUP_BASE_DIR="/home/$(whoami)/kafka-backups"

echo "🔧 Configuring Kafka Data Persistence"
echo "====================================="
echo ""
echo "🎯 Goals:"
echo "  • Data survives VM shutdown/restart"
echo "  • Persistent storage configuration"
echo "  • Manual backup/restore capabilities"
echo ""

# =====================================================
# Step 1: Create Persistent Data Directories
# =====================================================

setup_persistent_directories() {
    echo "📁 Setting up persistent data directories..."
    
    # Create Kafka data directory
    if [ ! -d "$KAFKA_DATA_DIR" ]; then
        sudo mkdir -p "$KAFKA_DATA_DIR"
        echo "✅ Created Kafka data directory: $KAFKA_DATA_DIR"
    else
        echo "✅ Kafka data directory exists: $KAFKA_DATA_DIR"
    fi
    
    # Create Zookeeper data directory
    if [ ! -d "$ZOOKEEPER_DATA_DIR" ]; then
        sudo mkdir -p "$ZOOKEEPER_DATA_DIR"
        echo "✅ Created Zookeeper data directory: $ZOOKEEPER_DATA_DIR"
    else
        echo "✅ Zookeeper data directory exists: $ZOOKEEPER_DATA_DIR"
    fi
    
    # Set proper ownership
    sudo chown -R $(whoami):$(whoami) "$KAFKA_DATA_DIR" "$ZOOKEEPER_DATA_DIR"
    echo "✅ Set ownership to current user"
    
    # Create manual backup directory
    mkdir -p "$BACKUP_BASE_DIR"
    echo "✅ Created backup directory"
    
    echo ""
}

# =====================================================
# Step 2: Configure Kafka for Persistent Storage
# =====================================================

configure_kafka_persistence() {
    echo "⚙️ Configuring Kafka for persistent storage..."
    
    # Backup current config
    cp "$KAFKA_CONFIG" "$KAFKA_CONFIG.backup.$(date +%Y%m%d_%H%M%S)"
    echo "✅ Backed up current Kafka config"
    
    # Update or add persistent storage settings
    update_or_add_config() {
        local key=$1
        local value=$2
        local file=$3
        
        if grep -q "^$key=" "$file"; then
            sed -i "s|^$key=.*|$key=$value|" "$file"
            echo "  ✅ Updated: $key=$value"
        else
            echo "$key=$value" >> "$file"
            echo "  ✅ Added: $key=$value"
        fi
    }
    
    # Configure persistent data directory
    update_or_add_config "log.dirs" "$KAFKA_DATA_DIR" "$KAFKA_CONFIG"
    
    # Configure for data safety and persistence
    update_or_add_config "num.recovery.threads.per.data.dir" "2" "$KAFKA_CONFIG"
    update_or_add_config "log.flush.interval.messages" "10000" "$KAFKA_CONFIG"
    update_or_add_config "log.flush.interval.ms" "1000" "$KAFKA_CONFIG"
    update_or_add_config "log.flush.scheduler.interval.ms" "3000" "$KAFKA_CONFIG"
    
    # Configure for durability (prevent data loss)
    update_or_add_config "default.replication.factor" "1" "$KAFKA_CONFIG"
    update_or_add_config "min.insync.replicas" "1" "$KAFKA_CONFIG"
    update_or_add_config "unclean.leader.election.enable" "false" "$KAFKA_CONFIG"
    
    # Configure proper shutdown behavior
    update_or_add_config "controlled.shutdown.enable" "true" "$KAFKA_CONFIG"
    update_or_add_config "controlled.shutdown.max.retries" "3" "$KAFKA_CONFIG"
    update_or_add_config "controlled.shutdown.retry.backoff.ms" "5000" "$KAFKA_CONFIG"
    
    # Configure log retention for persistence (3 months)
    update_or_add_config "log.retention.ms" "7776000000" "$KAFKA_CONFIG"
    update_or_add_config "log.retention.bytes" "107374182400" "$KAFKA_CONFIG"
    update_or_add_config "compression.type" "none" "$KAFKA_CONFIG"
    
    echo "✅ Kafka configured for persistent storage"
    echo ""
}

# =====================================================
# Step 3: Configure Zookeeper for Persistent Storage
# =====================================================

configure_zookeeper_persistence() {
    echo "⚙️ Configuring Zookeeper for persistent storage..."
    
    # Backup current config
    cp "$ZOOKEEPER_CONFIG" "$ZOOKEEPER_CONFIG.backup.$(date +%Y%m%d_%H%M%S)"
    echo "✅ Backed up current Zookeeper config"
    
    # Update Zookeeper data directory
    if grep -q "^dataDir=" "$ZOOKEEPER_CONFIG"; then
        sed -i "s|^dataDir=.*|dataDir=$ZOOKEEPER_DATA_DIR|" "$ZOOKEEPER_CONFIG"
        echo "  ✅ Updated dataDir=$ZOOKEEPER_DATA_DIR"
    else
        echo "dataDir=$ZOOKEEPER_DATA_DIR" >> "$ZOOKEEPER_CONFIG"
        echo "  ✅ Added dataDir=$ZOOKEEPER_DATA_DIR"
    fi
    
    # Configure Zookeeper persistence settings
    if ! grep -q "^dataLogDir=" "$ZOOKEEPER_CONFIG"; then
        echo "dataLogDir=$ZOOKEEPER_DATA_DIR/logs" >> "$ZOOKEEPER_CONFIG"
        echo "  ✅ Added dataLogDir for transaction logs"
    fi
    
    # Configure snapshots and cleanup
    if ! grep -q "^autopurge.snapRetainCount=" "$ZOOKEEPER_CONFIG"; then
        echo "autopurge.snapRetainCount=10" >> "$ZOOKEEPER_CONFIG"
        echo "autopurge.purgeInterval=24" >> "$ZOOKEEPER_CONFIG"
        echo "  ✅ Added snapshot retention and cleanup"
    fi
    
    echo "✅ Zookeeper configured for persistent storage"
    echo ""
}

# =====================================================
# Step 4: Create Manual Backup/Restore Scripts
# =====================================================

create_manual_backup_scripts() {
    echo "💾 Creating manual backup/restore scripts..."
    
    # Manual backup script
    cat > "$BACKUP_BASE_DIR/manual_backup.sh" << 'EOF'
#!/bin/bash
# Manual Kafka Data Backup Script

KAFKA_DATA_DIR="/var/kafka-data"
ZOOKEEPER_DATA_DIR="/var/zookeeper-data"
BACKUP_DIR="/home/$(whoami)/kafka-backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "🔄 Starting manual Kafka backup..."
echo "Backup will be saved with timestamp: $TIMESTAMP"

# Create backup directory
mkdir -p "$BACKUP_DIR/$TIMESTAMP"

# Backup Kafka data
if [ -d "$KAFKA_DATA_DIR" ]; then
    echo "📁 Backing up Kafka data..."
    tar -czf "$BACKUP_DIR/$TIMESTAMP/kafka-data.tar.gz" -C "$(dirname $KAFKA_DATA_DIR)" "$(basename $KAFKA_DATA_DIR)" 2>/dev/null
    echo "✅ Kafka data backed up"
fi

# Backup Zookeeper data
if [ -d "$ZOOKEEPER_DATA_DIR" ]; then
    echo "📁 Backing up Zookeeper data..."
    tar -czf "$BACKUP_DIR/$TIMESTAMP/zookeeper-data.tar.gz" -C "$(dirname $ZOOKEEPER_DATA_DIR)" "$(basename $ZOOKEEPER_DATA_DIR)" 2>/dev/null
    echo "✅ Zookeeper data backed up"
fi

echo "✅ Manual backup completed: $BACKUP_DIR/$TIMESTAMP"
echo ""
echo "📋 Backup contents:"
ls -lh "$BACKUP_DIR/$TIMESTAMP/"
EOF

    # Manual restore script
    cat > "$BACKUP_BASE_DIR/manual_restore.sh" << 'EOF'
#!/bin/bash
# Manual Kafka Data Restore Script

BACKUP_BASE_DIR="/home/$(whoami)/kafka-backups"
KAFKA_DATA_DIR="/var/kafka-data"
ZOOKEEPER_DATA_DIR="/var/zookeeper-data"

echo "🔄 Kafka Manual Data Restore Tool"
echo "=================================="

# Check if services are running
if pgrep -f "kafka.Kafka\|QuorumPeerMain" > /dev/null; then
    echo "❌ Please stop Kafka and Zookeeper first:"
    echo "   ./kafka-control.sh stop"
    exit 1
fi

# List available backups
echo "📋 Available backups:"
echo ""
if [ -d "$BACKUP_BASE_DIR" ]; then
    ls -la "$BACKUP_BASE_DIR/" | grep "^d" | awk '{print $9}' | grep -E "^[0-9]{8}_[0-9]{6}$" | nl -w3 -s'. ' || echo "  No backups found"
else
    echo "  No backup directory found"
    exit 1
fi

echo ""
read -p "Enter backup timestamp (e.g., 20241111_143022): " BACKUP_NAME

# Validate backup exists
BACKUP_PATH="$BACKUP_BASE_DIR/$BACKUP_NAME"
if [ ! -d "$BACKUP_PATH" ]; then
    echo "❌ Backup not found: $BACKUP_NAME"
    exit 1
fi

echo "🔄 Restoring from: $BACKUP_PATH"

# Backup current data (if exists)
if [ -d "$KAFKA_DATA_DIR" ] || [ -d "$ZOOKEEPER_DATA_DIR" ]; then
    echo "💾 Backing up current data before restore..."
    CURRENT_BACKUP="$BACKUP_BASE_DIR/pre-restore-$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$CURRENT_BACKUP"
    
    [ -d "$KAFKA_DATA_DIR" ] && sudo mv "$KAFKA_DATA_DIR" "$CURRENT_BACKUP/kafka-data-old"
    [ -d "$ZOOKEEPER_DATA_DIR" ] && sudo mv "$ZOOKEEPER_DATA_DIR" "$CURRENT_BACKUP/zookeeper-data-old"
    echo "✅ Current data backed up to: $CURRENT_BACKUP"
fi

# Restore data
echo "🔄 Restoring Kafka data..."
if [ -f "$BACKUP_PATH/kafka-data.tar.gz" ]; then
    sudo tar -xzf "$BACKUP_PATH/kafka-data.tar.gz" -C "/"
    echo "✅ Kafka data restored"
fi

echo "🔄 Restoring Zookeeper data..."
if [ -f "$BACKUP_PATH/zookeeper-data.tar.gz" ]; then
    sudo tar -xzf "$BACKUP_PATH/zookeeper-data.tar.gz" -C "/"
    echo "✅ Zookeeper data restored"
fi

# Fix ownership
sudo chown -R $(whoami):$(whoami) "$KAFKA_DATA_DIR" "$ZOOKEEPER_DATA_DIR" 2>/dev/null

echo ""
echo "✅ Data restoration completed!"
echo "You can now start Kafka: ./kafka-control.sh start"
EOF

    # Graceful shutdown script
    cat > "$BACKUP_BASE_DIR/graceful_shutdown.sh" << 'EOF'
#!/bin/bash
# Graceful Kafka Shutdown Script
# Call this before VM shutdown to ensure data safety

echo "🛑 Graceful Kafka shutdown starting..."

# Stop Kafka first (gives time to flush data)
if pgrep -f "kafka.Kafka" > /dev/null; then
    echo "🔄 Stopping Kafka..."
    /opt/kafka/kafka_2.13-3.9.0/bin/kafka-server-stop.sh
    
    # Wait for Kafka to fully stop
    for i in {1..30}; do
        if ! pgrep -f "kafka.Kafka" > /dev/null; then
            echo "✅ Kafka stopped gracefully"
            break
        fi
        echo "  Waiting for Kafka to stop... ($i/30)"
        sleep 2
    done
fi

# Stop Zookeeper
if pgrep -f "QuorumPeerMain" > /dev/null; then
    echo "🔄 Stopping Zookeeper..."
    /opt/kafka/kafka_2.13-3.9.0/bin/zookeeper-server-stop.sh
    
    # Wait for Zookeeper to stop
    for i in {1..15}; do
        if ! pgrep -f "QuorumPeerMain" > /dev/null; then
            echo "✅ Zookeeper stopped gracefully"
            break
        fi
        echo "  Waiting for Zookeeper to stop... ($i/15)"
        sleep 2
    done
fi

echo "✅ Graceful shutdown completed - VM is safe to close"
EOF

    # Make scripts executable
    chmod +x "$BACKUP_BASE_DIR"/*.sh
    
    echo "✅ Created manual backup and recovery scripts"
    echo "  📁 Manual backup: $BACKUP_BASE_DIR/manual_backup.sh"
    echo "  🔄 Manual restore: $BACKUP_BASE_DIR/manual_restore.sh"
    echo "  🛑 Graceful shutdown: $BACKUP_BASE_DIR/graceful_shutdown.sh"
    echo ""
}

# =====================================================
# Main Setup Function
# =====================================================

main_setup() {
    echo "Starting Kafka persistence configuration..."
    echo ""
    
    # Check if Kafka is running
    if pgrep -f "kafka.Kafka\|QuorumPeerMain" > /dev/null; then
        echo "⚠️  Kafka/Zookeeper is running. Stopping for configuration..."
        if [ -f "kafka-control.sh" ]; then
            ./kafka-control.sh stop
        else
            echo "❌ kafka-control.sh not found. Please stop Kafka manually:"
            echo "   pkill -f kafka.Kafka"
            echo "   pkill -f QuorumPeerMain"
            exit 1
        fi
        sleep 5
    fi
    
    # Run setup steps
    setup_persistent_directories
    configure_kafka_persistence
    configure_zookeeper_persistence
    create_manual_backup_scripts
    
    # Show summary
    show_setup_summary
}

# =====================================================
# Setup Summary
# =====================================================

show_setup_summary() {
    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo "🎉 Kafka Data Persistence Setup Complete!"
    echo "════════════════════════════════════════════════════════════"
    echo ""
    echo "📁 Data Directories:"
    echo "   Kafka:     $KAFKA_DATA_DIR"
    echo "   Zookeeper: $ZOOKEEPER_DATA_DIR"
    echo ""
    echo "🛠️ Manual Tools:"
    echo "   Backup:    $BACKUP_BASE_DIR/manual_backup.sh"
    echo "   Restore:   $BACKUP_BASE_DIR/manual_restore.sh"
    echo ""
    echo "🔧 Next Steps:"
    echo "   1. Start Kafka: ./kafka-control.sh start"
    echo "   2. Apply topic config: ./configure_kafka_retention.sh --apply-topic-config"
    echo "   3. Test persistence: ./verify_kafka_persistence.sh --test"
    echo ""
    echo "🚨 Before VM Shutdown - ALWAYS run:"
    echo "   $BACKUP_BASE_DIR/graceful_shutdown.sh"
    echo "════════════════════════════════════════════════════════════"
}

# =====================================================
# Command Line Interface
# =====================================================

case "$1" in
    --setup|"")
        main_setup
        ;;
    --backup)
        if [ -f "$BACKUP_BASE_DIR/manual_backup.sh" ]; then
            "$BACKUP_BASE_DIR/manual_backup.sh"
        else
            echo "❌ Backup script not found. Run setup first: $0 --setup"
        fi
        ;;
    --restore)
        if [ -f "$BACKUP_BASE_DIR/manual_restore.sh" ]; then
            "$BACKUP_BASE_DIR/manual_restore.sh"
        else
            echo "❌ Restore script not found. Run setup first: $0 --setup"
        fi
        ;;
    --shutdown)
        if [ -f "$BACKUP_BASE_DIR/graceful_shutdown.sh" ]; then
            "$BACKUP_BASE_DIR/graceful_shutdown.sh"
        else
            echo "❌ Shutdown script not found. Run setup first: $0 --setup"
        fi
        ;;
    --help|-h)
        echo "Simplified Kafka Data Persistence Setup"
        echo "Usage: $0 [option]"
        echo ""
        echo "Options:"
        echo "  --setup     Configure persistent storage (default)"
        echo "  --backup    Run manual backup"
        echo "  --restore   Restore from backup"
        echo "  --shutdown  Graceful shutdown before VM close"
        echo "  --help      Show this help"
        ;;
    *)
        echo "Unknown option: $1"
        echo "Use --help for usage information"
        exit 1
        ;;
esac