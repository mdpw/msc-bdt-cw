## Correct sequence for clean slate kafka setup

# Go to dinesh@dinesh-VirtualBox:/opt/kafka/kafka_2.13-3.9.0/bin$ 
./kafka-topics.sh --bootstrap-server 192.168.1.38:9092 --list

# Step 2: Delete Destination Kafka Topics
./kafka-topics.sh --bootstrap-server 192.168.1.38:9092 --delete --topic hourly-vehicle-metrics
./kafka-topics.sh --bootstrap-server 192.168.1.38:9092 --delete --topic daily-peak-metrics  
./kafka-topics.sh --bootstrap-server 192.168.1.38:9092 --delete --topic sensor-availability-metrics

# Step 1: See all consumer groups
./kafka-consumer-groups.sh --bootstrap-server 192.168.1.38:9092 --list

# Step 2: Delete Consumer Groups
./kafka-consumer-groups.sh --bootstrap-server 192.168.1.38:9092 --delete --group traffic-consumer

# Step 3: Clear PostgreSQL Tables
# Use db-clean-up-commands.sql file commands

# This way consumer will process ALL messages in source topic and generate complete metrics from scratch