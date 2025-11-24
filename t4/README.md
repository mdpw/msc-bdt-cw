# Social Media Hashtag Counter - Big Data Streaming Pipeline

A real-time streaming analytics system that processes social media data (Facebook and Twitter) to count hashtag occurrences using Apache Kafka, Apache Flink, and Docker.

## Quick Start

Follow these commands in order to deploy and run the system:

### 1. Start Infrastructure
```bash
docker-compose up -d
```

### 2. Create Kafka Topics  
```bash
./create-topics.sh
```

### 3. Build Flink Jobs
```bash
mvn clean package
```

### 4. Deploy JAR to Flink
```bash
docker cp target/flink-hashtag-counter-1.0-SNAPSHOT.jar flink-jobmanager:/opt/flink/
```

### 5. Start Data Producer
```bash
python producer.py
```

### 6. Submit Flink Jobs
```bash
# Submit Twitter Hashtag Counter
docker exec -it flink-jobmanager flink run -c com.example.TwitterHashtagCounter /opt/flink/flink-hashtag-counter-1.0-SNAPSHOT.jar

# Submit Facebook Hashtag Counter  
docker exec -it flink-jobmanager flink run -c com.example.FacebookHashtagCounter /opt/flink/flink-hashtag-counter-1.0-SNAPSHOT.jar
```

### 7. Monitor Hashtag Counts
```bash
# Check TaskManager 1 logs
docker logs -f flink-taskmanager1 | grep -E "(FACEBOOK_HASHTAG_COUNTS|TWITTER_HASHTAG_COUNTS)"

# Check TaskManager 2 logs  
docker logs -f flink-taskmanager2 | grep -E "(FACEBOOK_HASHTAG_COUNTS|TWITTER_HASHTAG_COUNTS)"
```

## Expected Output

You should see real-time hashtag counts like:
```
FACEBOOK_HASHTAG_COUNTS> (internationalroaming,10)
FACEBOOK_HASHTAG_COUNTS> (mobileservice,10)
FACEBOOK_HASHTAG_COUNTS> (travel,10)
FACEBOOK_HASHTAG_COUNTS> (telecom,27)
FACEBOOK_HASHTAG_COUNTS> (customerservice,43)
TWITTER_HASHTAG_COUNTS> (socialmedia,104)
TWITTER_HASHTAG_COUNTS> (cinema,1)
TWITTER_HASHTAG_COUNTS> (movies,1)
TWITTER_HASHTAG_COUNTS> (laligaeasports,1)
TWITTER_HASHTAG_COUNTS> (ibrox125,1)
TWITTER_HASHTAG_COUNTS> (celticfc,1)
TWITTER_HASHTAG_COUNTS> (worldnews,1)

```

You should see accuracy, performace metrices like:
```
==================================================
FACEBOOK METRICS (Messages: 200)
==================================================
ACCURACY:
   Messages with hashtags: 194 (97.0%)
   Total hashtags found: 716
   Avg hashtags per message: 3.58
   Parse errors: 0 (0.00%)
PERFORMANCE:
   Avg latency: 0.43 ms
   Min latency: 0 ms
   Max latency: 20 ms
   Throughput: 2326 msg/sec
==================================================


==================================================
TWITTER METRICS (Messages: 200)
==================================================
ACCURACY:
   Messages with hashtags: 200 (100.0%)
   Total hashtags found: 612
   Avg hashtags per message: 3.06
   Parse errors: 0 (0.00%)
PERFORMANCE:
   Avg latency: 0.57 ms
   Min latency: 0 ms
   Max latency: 18 ms
   Throughput: 1754 msg/sec
==================================================

```

## Web Interfaces

- **Flink Dashboard**: http://localhost:8081
- **Kafdrop (Kafka UI)**: http://localhost:9000

## Features

- Real-time hashtag counting every 15 seconds
- Separate processing for Facebook and Twitter streams
- Fault-tolerant distributed processing
- Web-based monitoring and management
- Configurable streaming parameters

## Troubleshooting

If you don't see hashtag counts:
1. Ensure producer.py is running
2. Check Flink jobs are active in web UI
3. Verify Kafka topics have data in Kafdrop
4. Monitor both TaskManager logs
