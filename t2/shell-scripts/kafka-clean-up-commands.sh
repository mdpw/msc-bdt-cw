#!/bin/bash
## Correct sequence for clean slate kafka setup

# Navigate to Kafka bin directory
cd /opt/kafka/kafka_2.13-3.9.0/bin

echo "=========================================="
echo " Kafka Clean-Up Script"
echo "=========================================="
echo ""

# Step 1: List all current topics
echo "Step 1: Listing all topics..."
./kafka-topics.sh --bootstrap-server 192.168.1.38:9092 --list
echo ""

# Step 2: Delete Destination Kafka Topics
echo "Step 2: Deleting destination Kafka topics..."
./kafka-topics.sh --bootstrap-server 192.168.1.38:9092 --delete --topic hourly-rolling-metrics
./kafka-topics.sh --bootstrap-server 192.168.1.38:9092 --delete --topic daily-peak-metrics  
./kafka-topics.sh --bootstrap-server 192.168.1.38:9092 --delete --topic sensor-availability-metrics
echo ""

# Step 3: See all consumer groups
echo "Step 3: Listing all consumer groups..."
./kafka-consumer-groups.sh --bootstrap-server 192.168.1.38:9092 --list
echo ""

# Step 4: Delete Consumer Groups
echo "Step 4: Deleting consumer group..."
./kafka-consumer-groups.sh --bootstrap-server 192.168.1.38:9092 --delete --group traffic-consumer
echo ""

# Step 3: Clear PostgreSQL Tables
# Use db-clean-up-commands.sql file commands

# This way consumer will process ALL messages in source topic and generate complete metrics from scratch