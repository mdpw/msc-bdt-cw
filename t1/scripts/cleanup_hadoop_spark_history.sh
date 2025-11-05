#!/bin/bash

# Complete Hadoop/Spark Job History Cleanup Script
# This script will completely wipe all job history from all storage locations

set -e  # Exit on error

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() { echo -e "${BLUE}[INFO]${NC} $1"; }
print_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[WARNING]${NC} $1"; }
print_error() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "🧹 COMPLETE HADOOP/SPARK JOB HISTORY CLEANUP"
echo "============================================="

# Step 1: Stop all services
print_status "Stopping all Hadoop and Spark services..."
stop-yarn.sh 2>/dev/null || print_warning "YARN may not have been running"
stop-dfs.sh 2>/dev/null || print_warning "HDFS may not have been running"
mapred --daemon stop historyserver 2>/dev/null || print_warning "MapReduce History Server may not have been running"

# Try multiple ways to stop Spark History Server
stop-history-server.sh 2>/dev/null || \
$SPARK_HOME/sbin/stop-history-server.sh 2>/dev/null || \
/opt/spark/sbin/stop-history-server.sh 2>/dev/null || \
print_warning "Spark History Server stop script not found"

# Kill any remaining processes
print_status "Killing any remaining history server processes..."
pkill -f "JobHistoryServer" 2>/dev/null || true
pkill -f "HistoryServer" 2>/dev/null || true
pkill -f "org.apache.spark.deploy.history" 2>/dev/null || true

sleep 3

# Step 2: Clean all local filesystem locations
print_status "Cleaning local filesystem job history..."

# YARN logs and history
rm -rf $HADOOP_HOME/logs/userlogs/* 2>/dev/null || true
rm -rf $HADOOP_HOME/logs/*.log* 2>/dev/null || true
rm -rf $HADOOP_HOME/logs/*.out* 2>/dev/null || true
rm -rf /tmp/hadoop-$USER/yarn/local/userlogs/* 2>/dev/null || true
rm -rf /tmp/hadoop-yarn-timeline/* 2>/dev/null || true
rm -rf /tmp/hadoop-yarn/* 2>/dev/null || true

# MapReduce Job History (all possible locations)
rm -rf /tmp/hadoop-yarn/staging/history/* 2>/dev/null || true
rm -rf /tmp/hadoop-mapred/history/* 2>/dev/null || true
rm -rf /tmp/mapred/history/* 2>/dev/null || true
rm -rf $HADOOP_HOME/logs/history/* 2>/dev/null || true
rm -rf /var/log/hadoop-mapreduce/history/* 2>/dev/null || true

# Spark logs and history
rm -rf /tmp/spark-events/* 2>/dev/null || true
rm -rf $SPARK_HOME/logs/* 2>/dev/null || true
rm -rf $SPARK_HOME/work/* 2>/dev/null || true
rm -rf /tmp/spark-* 2>/dev/null || true
rm -rf /var/log/spark/* 2>/dev/null || true

# Application timeline service
rm -rf /tmp/hadoop-yarn/timeline/* 2>/dev/null || true

print_success "Local filesystem cleanup completed"

# Step 3: Start HDFS to clean HDFS locations
print_status "Starting HDFS to clean HDFS job history..."
start-dfs.sh
sleep 10

# Wait for HDFS to be ready
print_status "Waiting for HDFS to be ready..."
for i in {1..30}; do
    if hdfs dfs -ls / >/dev/null 2>&1; then
        break
    fi
    sleep 2
done

# Step 4: Clean all HDFS job history locations
print_status "Cleaning HDFS job history locations..."

# Get the current user
CURRENT_USER=$(whoami)

# MapReduce Job History in HDFS (all possible paths)
hdfs dfs -rm -r -f /tmp/hadoop-yarn/staging/history 2>/dev/null || true
hdfs dfs -rm -r -f /user/history 2>/dev/null || true
hdfs dfs -rm -r -f /mr-history 2>/dev/null || true
hdfs dfs -rm -r -f /user/$CURRENT_USER/.staging 2>/dev/null || true
hdfs dfs -rm -r -f /tmp/hadoop-mapred/history 2>/dev/null || true
hdfs dfs -rm -r -f /tmp/logs 2>/dev/null || true

# Yarn application logs in HDFS
hdfs dfs -rm -r -f /tmp/logs 2>/dev/null || true
hdfs dfs -rm -r -f /app-logs 2>/dev/null || true
hdfs dfs -rm -r -f /user/$CURRENT_USER/logs 2>/dev/null || true

# Timeline service data
hdfs dfs -rm -r -f /ats 2>/dev/null || true
hdfs dfs -rm -r -f /yarn/timeline 2>/dev/null || true

# Spark History in HDFS
hdfs dfs -rm -r -f /spark-history 2>/dev/null || true
hdfs dfs -rm -r -f /shared/spark-logs 2>/dev/null || true
hdfs dfs -rm -r -f /user/spark/applicationHistory 2>/dev/null || true

print_success "HDFS cleanup completed"

# Step 5: Recreate necessary directories
print_status "Recreating necessary directories..."

# Recreate directories with proper permissions
hdfs dfs -mkdir -p /tmp 2>/dev/null || true
hdfs dfs -chmod 1777 /tmp 2>/dev/null || true

# Recreate user directory
hdfs dfs -mkdir -p /user/$CURRENT_USER 2>/dev/null || true
hdfs dfs -chown $CURRENT_USER:$CURRENT_USER /user/$CURRENT_USER 2>/dev/null || true

# Recreate Spark events directory
mkdir -p /tmp/spark-events 2>/dev/null || true
chmod 777 /tmp/spark-events 2>/dev/null || true

print_success "Directories recreated"

# Step 6: Restart all services
print_status "Restarting all services..."

# Start YARN
start-yarn.sh
sleep 5

# Start MapReduce History Server
mapred --daemon start historyserver
sleep 3

# Start Spark History Server (try multiple locations)
if command -v start-history-server.sh >/dev/null 2>&1; then
    start-history-server.sh 2>/dev/null || print_warning "Spark History Server start failed"
elif [ -n "$SPARK_HOME" ] && [ -f "$SPARK_HOME/sbin/start-history-server.sh" ]; then
    $SPARK_HOME/sbin/start-history-server.sh 2>/dev/null || print_warning "Spark History Server start failed"
elif [ -f "/opt/spark/sbin/start-history-server.sh" ]; then
    /opt/spark/sbin/start-history-server.sh 2>/dev/null || print_warning "Spark History Server start failed"
else
    print_warning "Spark History Server script not found"
fi

print_success "All services restarted"

# Step 7: Verification
print_status "Waiting for services to fully start..."
sleep 15

echo ""
echo "🎯 VERIFICATION - Check these URLs (should show no job history):"
echo "📊 YARN Applications: http://localhost:8088/cluster/apps"
echo "📋 MapReduce Job History: http://localhost:19888/jobhistory"
echo "⚡ Spark History: http://localhost:18080"

echo ""
print_status "Checking service status..."

# Check YARN
if curl -s http://localhost:8088/ws/v1/cluster/apps >/dev/null 2>&1; then
    YARN_APPS=$(curl -s http://localhost:8088/ws/v1/cluster/apps | grep -o '"totalApplications":[0-9]*' | cut -d: -f2 2>/dev/null || echo "0")
    echo "📊 YARN Applications count: $YARN_APPS"
else
    print_warning "YARN ResourceManager not accessible"
fi

# Check MapReduce History
if curl -s http://localhost:19888/ws/v1/history/info >/dev/null 2>&1; then
    print_success "MapReduce Job History Server is running"
else
    print_warning "MapReduce Job History Server not accessible"
fi

# Check Spark History
if curl -s http://localhost:18080 >/dev/null 2>&1; then
    print_success "Spark History Server is running"
else
    print_warning "Spark History Server not accessible"
fi

echo ""
print_success "✅ COMPLETE CLEANUP FINISHED!"
echo ""
echo "🔄 If job history still appears, it may be cached in your browser."
echo "💡 Try refreshing the pages or opening in incognito/private mode."
echo ""
echo "📝 To verify cleanup worked, run a new job and check if only"
echo "   the new job appears in the history (not old ones)."