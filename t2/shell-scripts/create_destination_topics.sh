./kafka-topics.sh --create \
  --topic hourly-vehicle-metrics \
  --bootstrap-server 192.168.1.38:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=7776000000 \
  --config cleanup.policy=delete

./kafka-topics.sh --create \
  --topic daily-peak-metrics \
  --bootstrap-server 192.168.1.38:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=7776000000 \
  --config cleanup.policy=delete

./kafka-topics.sh --create \
  --topic sensor-availability-metrics \
  --bootstrap-server 192.168.1.38:9092 \
  --partitions 1 \
  --replication-factor 1 \
  --config retention.ms=7776000000 \
  --config cleanup.policy=delete