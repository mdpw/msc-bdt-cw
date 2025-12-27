# Social Media Hashtag Counter - Big Data Streaming Pipeline

A real-time streaming analytics system that processes social media data (Facebook and Twitter) to count hashtag occurrences using Apache Kafka, Apache Flink, and Docker.

This project implements **two experiments**:
- **Step 3**: Baseline performance with single-partition topics
- **Step 4**: Scaling performance with multi-partition topics and parallelization

---

## Quick Start

### Initial Setup (Do Once)

#### 1. Start Infrastructure
```bash
docker-compose up -d
```

#### 2. Build Flink Application
```bash
# From project root directory
mvn clean package
```

#### 3. Deploy JAR to Flink
```bash
docker cp target/flink-hashtag-counter-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/
```

---

## Step 3: Baseline Performance (Single Partition)

This step establishes baseline metrics with single-partition topics and no parallelization.

### Step 3.1: Create Single-Partition Topics
```bash
./create-topics.sh
```

**Verify topics created correctly:**
```bash
docker exec kafka kafka-topics --describe --topic twitter-posts --bootstrap-server localhost:9092
docker exec kafka kafka-topics --describe --topic facebook-posts --bootstrap-server localhost:9092
```
**Expected**: `PartitionCount: 1` for both topics

### Step 3.2: Submit Baseline Flink Jobs
```bash
# Submit Twitter Hashtag Counter (Baseline)
docker exec -it flink-jobmanager flink run \
  -c com.example.TwitterHashtagCounter \
  /opt/flink/flink-hashtag-counter-1.0-SNAPSHOT.jar

# Submit Facebook Hashtag Counter (Baseline)
docker exec -it flink-jobmanager flink run \
  -c com.example.FacebookHashtagCounter \
  /opt/flink/flink-hashtag-counter-1.0-SNAPSHOT.jar
```

### Step 3.3: Start Data Producer
```bash
python producer.py
```

### Step 3.4: Verify Step 3 Configuration

**Check Flink Web UI** (http://localhost:8081):
- Go to "Running Jobs" → Click on job
- **Parallelism should show**: 1/1 (single thread)
- **TaskManagers tab**: Only 1 TaskManager actively processing

**Expected Characteristics:**
- Single partition per topic
- Parallelism = 1 (default)
- Only 1 TaskManager processing
- Event Time windows (15s tumbling)
- Watermark delay: 20s

### Step 3.5: Monitor Baseline Results

**See windowed hashtag counts (every 15 seconds):**
```bash
# Monitor live output
docker logs -f flink-taskmanager1 | grep "HASHTAG_COUNTS"
```

**Expected Output:**
```
FACEBOOK_HASHTAG_COUNTS> (customerservice,88)
FACEBOOK_HASHTAG_COUNTS> (telecom,65)
TWITTER_HASHTAG_COUNTS> (socialmedia,117)
TWITTER_HASHTAG_COUNTS> (trending,117)
```

**See performance metrics (every 100 messages):**
```bash
# View live metrics
docker exec -it flink-taskmanager1 tail -f /opt/flink/log/*out | grep -A 15 "METRICS"
```

**Expected Baseline Metrics:**
```
==================================================
TWITTER METRICS (Messages: 100)
==================================================
ACCURACY:
   Messages with hashtags: 99 (99.3%)
   Total hashtags found: 150
   Avg hashtags per message: 1.50
   Parse errors: 0 (0.00%)
PERFORMANCE:
   Avg latency: 0.45 ms
   Min latency: 0 ms
   Max latency: 12 ms
   Throughput: 2222 msg/sec
==================================================
```

### Step 3.6: Collect Baseline Data

**Record these metrics for comparison:**
- Total messages processed
- Average latency
- Throughput (msg/sec)
- Hashtag coverage percentage
- Resource utilization (from Flink Web UI)

---

## Step 4: Scaling Performance (Multiple Partitions)

This step measures performance improvements from parallelization with 2 partitions and 2-way parallel processing.

### Step 4.1: Stop Baseline Jobs
```bash
# List running jobs
docker exec flink-jobmanager flink list

# Cancel each job (use JOB_ID from list command)
docker exec flink-jobmanager flink cancel <TWITTER_JOB_ID>
docker exec flink-jobmanager flink cancel <FACEBOOK_JOB_ID>

# Or cancel all jobs via Web UI: http://localhost:8081
```

### Step 4.2: Delete Old Topics
```bash
docker exec kafka kafka-topics --delete --topic twitter-posts --bootstrap-server localhost:9092
docker exec kafka kafka-topics --delete --topic facebook-posts --bootstrap-server localhost:9092
```

**Wait 10-15 seconds for deletion to complete**

### Step 4.3: Create Multi-Partition Topics
```bash
./create-topics-scaling.sh
```

**Verify topics created correctly:**
```bash
docker exec kafka kafka-topics --describe --topic twitter-posts --bootstrap-server localhost:9092
docker exec kafka kafka-topics --describe --topic facebook-posts --bootstrap-server localhost:9092
```
**Expected**: `PartitionCount: 2` for both topics

### Step 4.4: Submit Scaling Flink Jobs
```bash
# Submit Twitter Hashtag Counter (Scaling)
docker exec -it flink-jobmanager flink run \
  -c com.example.TwitterHashtagCounterScaling \
  /opt/flink/flink-hashtag-counter-1.0-SNAPSHOT.jar

# Submit Facebook Hashtag Counter (Scaling)
docker exec -it flink-jobmanager flink run \
  -c com.example.FacebookHashtagCounterScaling \
  /opt/flink/flink-hashtag-counter-1.0-SNAPSHOT.jar
```

### Step 4.5: Start Data Producer
```bash
python producer.py
```

### Step 4.6: Verify Step 4 Configuration

**Check Flink Web UI** (http://localhost:8081):
- Go to "Running Jobs" → Click on job
- **Parallelism should show**: 2/2 (parallel threads)
- **TaskManagers tab**: BOTH TaskManagers actively processing

**Expected Characteristics:**
- 2 partitions per topic
- Parallelism = 2 (explicit)
- BOTH TaskManagers processing
- Event Time windows (15s tumbling - same as Step 3)
- Watermark delay: 20s (same as Step 3)

### Step 4.7: Monitor Scaling Results

**See windowed hashtag counts (every 15 seconds):**
```bash
# Monitor BOTH TaskManagers (data distributed across them)
docker logs -f flink-taskmanager1 | grep "STEP4_HASHTAG_COUNTS" &
docker logs -f flink-taskmanager2 | grep "STEP4_HASHTAG_COUNTS" &
```

**See performance metrics (every 100 messages):**
```bash
# View metrics from both TaskManagers
docker exec -it flink-taskmanager1 tail -f /opt/flink/log/*out | grep -A 18 "STEP 4 METRICS" &
docker exec -it flink-taskmanager2 tail -f /opt/flink/log/*out | grep -A 18 "STEP 4 METRICS" &
```

**Expected Improved Metrics:**
```
============================================================
TWITTER STEP 4 METRICS (Messages: 100)
SCALING EXPERIMENT: 2 Partitions + Parallelism
============================================================
ACCURACY:
   Messages with hashtags: 99 (99.3%)  ← Same as Step 3
   Total hashtags found: 150            ← Same as Step 3
   Avg hashtags per message: 1.50      ← Same as Step 3
   Parse errors: 0 (0.00%)             ← Same as Step 3
PERFORMANCE:
   Avg latency: 0.22 ms                ← IMPROVED (lower)
   Min latency: 0 ms
   Max latency: 6 ms                   ← IMPROVED (lower)
   Throughput: 4545 msg/sec            ← IMPROVED (higher)
SCALING METRICS:
   Partition messages: 50               ← Load distributed
   Parallelism: ENABLED (2 partitions)
   Watermarking: PARTITION-AWARE
============================================================
```

### Step 4.8: Collect Scaling Data

**Record these metrics for comparison with Step 3:**
- Total messages processed (should match Step 3)
- Average latency (should be lower than Step 3)
- Throughput (should be higher than Step 3)
- Hashtag coverage (should match Step 3 - ~99.3% Twitter, ~92.4% Facebook)
- Resource utilization across BOTH TaskManagers

---

## Performance Comparison Table

After completing both steps, create a comparison table:

| Metric | Step 3 (Baseline) | Step 4 (Scaling) | Improvement |
|--------|------------------|-----------------|-------------|
| **Partitions** | 1 per topic | 2 per topic | 2x |
| **Parallelism** | 1 | 2 | 2x |
| **Active TaskManagers** | 1 | 2 | 2x |
| **Avg Latency (Twitter)** | ___ ms | ___ ms | __% |
| **Throughput (Twitter)** | ___ msg/s | ___ msg/s | __% |
| **Avg Latency (Facebook)** | ___ ms | ___ ms | __% |
| **Throughput (Facebook)** | ___ msg/s | ___ msg/s | __% |
| **Accuracy (Twitter)** | ~99.3% | ~99.3% | No change  |
| **Accuracy (Facebook)** | ~92.4% | ~92.4% | No change  |

**Expected Results:**
- Latency: 30-50% reduction
- Throughput: 50-100% improvement
- Accuracy: Identical (validates fair comparison)

---

## Web Interfaces

- **Flink Dashboard**: http://localhost:8081
  - View job status, parallelism, TaskManager utilization
- **Kafdrop (Kafka UI)**: http://localhost:9000
  - Monitor topics, partitions, message throughput

---

## Configuration

All parameters are centralized in `config.yml`:

```yaml
flink:
  window_seconds: 15              # Tumbling window size (both steps)
  watermark_delay_seconds: 20     # Out-of-order event handling (both steps)
  parallelism: 2                  # Only used by Step 4 Scaling files
```

**Modify this file to experiment with different settings.**

---

## Architecture Differences

### Step 3: Baseline Architecture
```
Producer → Kafka (1 partition) → Flink (parallelism=1) → 1 TaskManager → Output
```

### Step 4: Scaling Architecture
```
Producer → Kafka (2 partitions) → Flink (parallelism=2) → 2 TaskManagers → Output
                ↓                                              ↓
          Partition 0 ────────────────────────────> TaskManager 1
          Partition 1 ────────────────────────────> TaskManager 2
```

---

## Key Features

- Real-time hashtag counting with 15-second tumbling windows
- Event Time processing with watermark-based out-of-order event handling
- Separate processing pipelines for Facebook and Twitter
- Fair performance comparison between single and parallel processing
- Comprehensive accuracy and performance metrics
- YAML-based configuration management
- Fault-tolerant distributed processing
- Web-based monitoring and management

---

## Troubleshooting

### No Hashtag Counts Appearing

**Check producer:**
```bash
# Ensure producer.py is running and sending data
ps aux | grep producer.py
```

**Check Flink jobs:**
```bash
# Verify jobs are running
docker exec flink-jobmanager flink list
```

**Check Kafka topics:**
```bash
# Verify data is being produced
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic twitter-posts \
  --from-beginning \
  --max-messages 5
```

### Jobs Not Starting

**Check JobManager logs:**
```bash
docker logs flink-jobmanager --tail 100
```

**Verify JAR is deployed:**
```bash
docker exec flink-jobmanager ls -lh /opt/flink/*.jar
```

### Partition Count Mismatch

**If you see only 1 partition when expecting 2:**
```bash
# Delete topics completely
docker exec kafka kafka-topics --delete --topic twitter-posts --bootstrap-server localhost:9092
docker exec kafka kafka-topics --delete --topic facebook-posts --bootstrap-server localhost:9092

# Wait 15 seconds, then recreate
./create-topics-scaling.sh

# Verify
docker exec kafka kafka-topics --describe --topic twitter-posts --bootstrap-server localhost:9092
```

### Step 4 Shows Parallelism = 1

**Ensure you're using the Scaling files:**
- Should submit `TwitterHashtagCounterScaling` (not `TwitterHashtagCounter`)
- Should submit `FacebookHashtagCounterScaling` (not `FacebookHashtagCounter`)

**Rebuild JAR if you made changes:**
```bash
mvn clean package
docker cp target/flink-hashtag-counter-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/
```

### Metrics Not Appearing

**TaskManager logs location:**
```bash
# Check all .out files
docker exec flink-taskmanager1 ls -lh /opt/flink/log/

# View specific log
docker exec flink-taskmanager1 cat /opt/flink/log/flink-*-taskexecutor-*.out
```

---

## Clean Shutdown

```bash
# Stop producer (Ctrl+C in producer terminal)

# Cancel all Flink jobs
docker exec flink-jobmanager flink list
docker exec flink-jobmanager flink cancel <JOB_ID>

# Stop Docker containers
docker-compose down

# Optional: Remove all data
docker-compose down -v
```

---
