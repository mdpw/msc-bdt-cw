#!/bin/bash

echo "Creating Kafka topics..."

# Create Facebook topic
docker exec kafka kafka-topics --create \
  --topic facebook-posts \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1

echo "Created topic: facebook-posts"

# Create Twitter topic
docker exec kafka kafka-topics --create \
  --topic twitter-posts \
  --bootstrap-server localhost:9092 \
  --partitions 2 \
  --replication-factor 1

echo "Created topic: twitter-posts"

# List all topics to verify
echo "Current topics:"
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo "Topics created successfully!"