#!/bin/bash
# Simple Kafka Topic Creation Script - CORRECTED
# Creates topics with 3-month retention and uncompressed data

cd /opt/kafka/kafka_2.13-3.9.0/bin

echo "🔧 Creating Kafka topics with 3-month retention (uncompressed)..."
echo "Server: 192.168.1.38:9092"
echo ""

# Function to create topic with retention (uncompressed)
create_topic_simple() {
    local topic_name=$1
    
    echo "📝 Setting up topic: $topic_name"
    
    # Check if topic exists
    if ./kafka-topics.sh --bootstrap-server 192.168.1.38:9092 --list | grep -q "^$topic_name$"; then
        echo "✅ Topic $topic_name already exists - updating retention only"
        
        # Update retention for existing topic
        ./kafka-configs.sh --bootstrap-server 192.168.1.38:9092 \
          --entity-type topics \
          --entity-name $topic_name \
          --alter \
          --add-config retention.ms=7776000000,compression.type=uncompressed
          
    else
        echo "🆕 Creating new topic: $topic_name"
        
        # Create topic with retention and uncompressed data
        ./kafka-topics.sh --bootstrap-server 192.168.1.38:9092 \
          --create \
          --topic $topic_name \
          --partitions 3 \
          --replication-factor 1 \
          --config retention.ms=7776000000 \
          --config retention.bytes=107374182400 \
          --config compression.type=uncompressed \
          --config cleanup.policy=delete
    fi
    
    if [ $? -eq 0 ]; then
        echo "✅ Topic $topic_name configured successfully"
    else
        echo "❌ Failed to configure topic $topic_name"
    fi
    echo ""
}

# Create all required topics
create_topic_simple "traffic-data"
create_topic_simple "hourly-rolling-metrics"
create_topic_simple "daily-peak-metrics"
create_topic_simple "sensor-availability-metrics"

echo "🎉 All topics configured!"
echo ""
echo "📋 Current topics:"
./kafka-topics.sh --bootstrap-server 192.168.1.38:9092 --list

echo ""
echo "🔍 Verifying retention settings:"
for topic in traffic-data hourly-rolling-metrics daily-peak-metrics sensor-availability-metrics; do
    echo "Topic: $topic"
    ./kafka-configs.sh --bootstrap-server 192.168.1.38:9092 \
      --entity-type topics \
      --entity-name $topic \
      --describe | grep -E "retention.ms|compression.type" || echo "  Configuration not found"
done

echo ""
echo "✅ Setup complete - ready for real-time streaming!"
echo "🚀 No compression libraries needed - uses uncompressed data!"