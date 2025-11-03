#!/bin/bash
echo "=== HADOOP READINESS CHECK ==="

# 1. Check Java (MOST IMPORTANT)
echo "1. Checking Java..."
if java -version 2>&1 | grep -q "version"; then
    echo "Java is installed"
    java -version
else
    echo "Java NOT installed - Install with:"
    echo "sudo apt update && sudo apt install openjdk-8-jdk"
fi

# 2. Check if Hadoop is installed
echo -e "\n2. Checking Hadoop..."
if command -v hadoop >/dev/null 2>&1; then
    echo "Hadoop is installed"
    hadoop version | head -1
else
    echo "Hadoop NOT installed"
fi

# 3. Check SSH
echo -e "\n3. Checking SSH..."
if systemctl is-active ssh >/dev/null 2>&1; then
    echo "SSH service is running"
else
    echo "SSH service not running - Start with:"
    echo "sudo systemctl start ssh"
fi

# 4. Check memory
echo -e "\n4. Checking system resources..."
echo "RAM: $(free -h | awk '/^Mem:/ {print $2}')"
echo "Disk: $(df -h / | awk 'NR==2 {print $4}' | head -1) available"

echo -e "\n=== END CHECK ==="