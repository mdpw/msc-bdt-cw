#!/bin/bash
# =====================================================
# Kafka IP Management Script
# Options: Use old IP, set permanent IP, or configure static IP
# =====================================================

KAFKA_HOME="/opt/kafka/kafka_2.13-3.9.0"
KAFKA_CONFIG="$KAFKA_HOME/config/server.properties"
OLD_IP="192.168.1.38"
CURRENT_IP=$(ip addr show | grep "inet " | grep -v "127.0.0.1" | grep "192.168" | awk '{print $2}' | cut -d'/' -f1 | head -1)

echo "🔧 Kafka IP Management Tool"
echo "============================"
echo "Old IP: $OLD_IP"
echo "Current IP: $CURRENT_IP"
echo ""

# =====================================================
# Option 1: Configure Static IP Address (Permanent)
# =====================================================

configure_static_ip() {
    local target_ip=$1
    
    echo "🔧 Configuring Static IP Address"
    echo "================================="
    echo "Target IP: $target_ip"
    echo ""
    
    # Detect network interface
    INTERFACE=$(ip route | grep default | awk '{print $5}' | head -1)
    echo "Detected network interface: $INTERFACE"
    
    # Detect current network configuration
    GATEWAY=$(ip route | grep default | awk '{print $3}' | head -1)
    NETMASK="24"  # Assuming /24 subnet, adjust if needed
    
    echo "Detected gateway: $GATEWAY"
    echo ""
    
    # Backup current network configuration
    echo "💾 Backing up current network configuration..."
    sudo cp /etc/netplan/00-installer-config.yaml "/etc/netplan/00-installer-config.yaml.backup.$(date +%Y%m%d_%H%M%S)" 2>/dev/null || echo "No netplan config found"
    
    # Create netplan configuration
    echo "📝 Creating static IP configuration..."
    
    cat << EOF | sudo tee /etc/netplan/01-static-ip.yaml > /dev/null
network:
  version: 2
  renderer: networkd
  ethernets:
    $INTERFACE:
      dhcp4: false
      addresses:
        - $target_ip/$NETMASK
      gateway4: $GATEWAY
      nameservers:
        addresses:
          - 8.8.8.8
          - 8.8.4.4
EOF

    echo "✅ Static IP configuration created"
    echo ""
    
    echo "📋 Configuration summary:"
    echo "   Interface: $INTERFACE"
    echo "   Static IP: $target_ip/$NETMASK"
    echo "   Gateway: $GATEWAY"
    echo "   DNS: 8.8.8.8, 8.8.4.4"
    echo ""
    
    echo "🔄 To apply the static IP configuration:"
    echo "   sudo netplan apply"
    echo ""
    echo "⚠️  WARNING: This will change your VM's IP address!"
    echo "   You may lose SSH connection if connecting remotely."
    echo "   Make sure you can access the VM console directly."
    echo ""
    
    read -p "Apply static IP configuration now? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "🔄 Applying static IP configuration..."
        sudo netplan apply
        
        echo "✅ Static IP applied!"
        echo "🔍 New IP should be: $target_ip"
        echo ""
        
        # Wait a moment for network to settle
        sleep 3
        
        # Check if IP changed
        NEW_IP=$(ip addr show | grep "inet " | grep -v "127.0.0.1" | grep "192.168" | awk '{print $2}' | cut -d'/' -f1 | head -1)
        if [ "$NEW_IP" = "$target_ip" ]; then
            echo "✅ IP successfully changed to: $NEW_IP"
        else
            echo "⚠️  IP may not have changed yet. Current: $NEW_IP"
            echo "   Try: ip addr show"
        fi
    else
        echo "❌ Static IP configuration created but not applied"
        echo "   Apply later with: sudo netplan apply"
    fi
}

# =====================================================
# Option 2: Configure Kafka for Specific IP
# =====================================================

configure_kafka_for_ip() {
    local target_ip=$1
    
    echo "⚙️ Configuring Kafka for IP: $target_ip"
    echo "======================================"
    
    # Backup current config
    cp "$KAFKA_CONFIG" "$KAFKA_CONFIG.backup.$(date +%Y%m%d_%H%M%S)"
    echo "✅ Backed up Kafka configuration"
    
    # Update Kafka configuration
    if grep -q "^listeners=" "$KAFKA_CONFIG"; then
        sed -i "s|^listeners=.*|listeners=PLAINTEXT://0.0.0.0:9092|" "$KAFKA_CONFIG"
    else
        echo "listeners=PLAINTEXT://0.0.0.0:9092" >> "$KAFKA_CONFIG"
    fi
    
    if grep -q "^advertised.listeners=" "$KAFKA_CONFIG"; then
        sed -i "s|^advertised.listeners=.*|advertised.listeners=PLAINTEXT://$target_ip:9092|" "$KAFKA_CONFIG"
    else
        echo "advertised.listeners=PLAINTEXT://$target_ip:9092" >> "$KAFKA_CONFIG"
    fi
    
    echo "✅ Updated Kafka configuration:"
    echo "   listeners=PLAINTEXT://0.0.0.0:9092"
    echo "   advertised.listeners=PLAINTEXT://$target_ip:9092"
    
    # Update config.yml if it exists
    if [ -f "config.yml" ]; then
        cp "config.yml" "config.yml.backup.$(date +%Y%m%d_%H%M%S)"
        sed -i "s/bootstrap_servers:.*/bootstrap_servers: ['$target_ip:9092']/" config.yml
        sed -i "s/[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}:9092/$target_ip:9092/g" config.yml
        echo "✅ Updated config.yml"
    fi
    
    echo ""
    echo "🔄 Restart Kafka to apply changes:"
    echo "   ./kafka-control.sh restart"
}

# =====================================================
# Option 3: Check What IP Kafka is Currently Using
# =====================================================

check_kafka_ip() {
    echo "🔍 Checking Kafka IP Configuration"
    echo "=================================="
    
    if [ -f "$KAFKA_CONFIG" ]; then
        echo "📋 Current Kafka configuration:"
        echo ""
        grep -E "^listeners=|^advertised.listeners=" "$KAFKA_CONFIG" || echo "   No listeners configured"
        
        echo ""
        echo "📋 config.yml configuration:"
        if [ -f "config.yml" ]; then
            grep -A 2 "bootstrap_servers:" config.yml 2>/dev/null || echo "   bootstrap_servers not found"
        else
            echo "   config.yml not found"
        fi
        
        echo ""
        echo "🔌 Testing connectivity:"
        
        # Test different IPs
        for ip in "$OLD_IP" "$CURRENT_IP" "localhost"; do
            echo -n "   Testing $ip:9092... "
            if timeout 5 "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$ip:9092" --list > /dev/null 2>&1; then
                echo "✅ WORKS"
                echo "     Topics: $("$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$ip:9092" --list 2>/dev/null | wc -l)"
            else
                echo "❌ Failed"
            fi
        done
    else
        echo "❌ Kafka configuration not found: $KAFKA_CONFIG"
    fi
}

# =====================================================
# Option 4: Revert to DHCP (Undo Static IP)
# =====================================================

revert_to_dhcp() {
    echo "🔄 Reverting to DHCP (Dynamic IP)"
    echo "================================="
    
    echo "⚠️  This will remove static IP configuration and use DHCP"
    echo "   Your IP may change on next reboot!"
    echo ""
    
    read -p "Continue with DHCP revert? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        # Remove static IP configuration
        if [ -f "/etc/netplan/01-static-ip.yaml" ]; then
            sudo rm /etc/netplan/01-static-ip.yaml
            echo "✅ Removed static IP configuration"
        fi
        
        # Apply changes
        sudo netplan apply
        echo "✅ Reverted to DHCP"
        echo ""
        
        # Check new IP
        sleep 3
        NEW_IP=$(ip addr show | grep "inet " | grep -v "127.0.0.1" | grep "192.168" | awk '{print $2}' | cut -d'/' -f1 | head -1)
        echo "🔍 Current IP: $NEW_IP"
    else
        echo "❌ DHCP revert cancelled"
    fi
}

# =====================================================
# Main Menu
# =====================================================

show_menu() {
    echo ""
    echo "🎯 Choose an option:"
    echo "==================="
    echo ""
    echo "1. 🔧 Set permanent static IP to $OLD_IP (recommended)"
    echo "2. ⚙️  Configure Kafka for current IP ($CURRENT_IP)"
    echo "3. ⚙️  Configure Kafka for old IP ($OLD_IP)"
    echo "4. 🔍 Check current Kafka IP configuration"
    echo "5. 🔄 Revert to DHCP (dynamic IP)"
    echo "6. ❌ Exit"
    echo ""
}

# =====================================================
# Command Line Interface
# =====================================================

case "$1" in
    --static-old)
        configure_static_ip "$OLD_IP"
        configure_kafka_for_ip "$OLD_IP"
        ;;
    --static-current)
        configure_static_ip "$CURRENT_IP"
        configure_kafka_for_ip "$CURRENT_IP"
        ;;
    --kafka-old)
        configure_kafka_for_ip "$OLD_IP"
        ;;
    --kafka-current)
        configure_kafka_for_ip "$CURRENT_IP"
        ;;
    --check)
        check_kafka_ip
        ;;
    --revert-dhcp)
        revert_to_dhcp
        ;;
    --help|-h)
        echo "Kafka IP Management Tool"
        echo "Usage: $0 [option]"
        echo ""
        echo "Options:"
        echo "  --static-old      Set static IP to old IP ($OLD_IP) and configure Kafka"
        echo "  --static-current  Set static IP to current IP ($CURRENT_IP) and configure Kafka"
        echo "  --kafka-old       Configure Kafka for old IP only"
        echo "  --kafka-current   Configure Kafka for current IP only"
        echo "  --check           Check current IP configuration"
        echo "  --revert-dhcp     Revert to DHCP (remove static IP)"
        echo "  --help            Show this help"
        echo ""
        echo "Interactive mode: $0 (no arguments)"
        ;;
    "")
        # Interactive mode
        while true; do
            show_menu
            read -p "Enter choice (1-6): " choice
            
            case $choice in
                1)
                    configure_static_ip "$OLD_IP"
                    configure_kafka_for_ip "$OLD_IP"
                    ;;
                2)
                    configure_kafka_for_ip "$CURRENT_IP"
                    ;;
                3)
                    configure_kafka_for_ip "$OLD_IP"
                    ;;
                4)
                    check_kafka_ip
                    ;;
                5)
                    revert_to_dhcp
                    ;;
                6)
                    echo "👋 Goodbye!"
                    exit 0
                    ;;
                *)
                    echo "❌ Invalid choice. Please enter 1-6."
                    ;;
            esac
            
            echo ""
            read -p "Press Enter to continue..."
        done
        ;;
    *)
        echo "Unknown option: $1"
        echo "Use --help for usage information"
        exit 1
        ;;
esac

echo ""
echo "════════════════════════════════════════════════════════════"
echo "📋 Summary"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "🔗 Key Points:"
echo "   • Static IP = Permanent (survives reboots)"
echo "   • DHCP IP = Can change on reboot"
echo "   • Kafka configuration must match your chosen IP"
echo ""
echo "🚀 Next Steps:"
echo "   1. Restart Kafka: ./kafka-control.sh restart"
echo "   2. Test connection: $KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server [IP]:9092 --list"
echo "   3. Run your producer/consumer with the configured IP"
echo ""
echo "💡 Tip: Use static IP for production/development environments"
echo "════════════════════════════════════════════════════════════"