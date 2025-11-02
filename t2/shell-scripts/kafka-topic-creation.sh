# cd /opt/kafka/kafka_2.13-3.9.0/bin

# Set 90 days retention
./kafka-configs.sh --bootstrap-server 192.168.1.38:9092 \
  --entity-type topics \
  --entity-name traffic-data \
  --alter \
  --add-config retention.ms=7776000000

# For hourly metrics topic
./kafka-configs.sh --bootstrap-server 192.168.1.38:9092 \
  --entity-type topics \
  --entity-name hourly-vehicle-metrics \
  --alter \
  --add-config retention.ms=7776000000

# For daily metrics topic  
./kafka-configs.sh --bootstrap-server 192.168.1.38:9092 \
  --entity-type topics \
  --entity-name daily-peak-metrics \
  --alter \
  --add-config retention.ms=7776000000

# For availability metrics topic
./kafka-configs.sh --bootstrap-server 192.168.1.38:9092 \
  --entity-type topics \
  --entity-name sensor-availability-metrics \
  --alter \
  --add-config retention.ms=7776000000