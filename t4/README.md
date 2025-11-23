# Social Media Hashtag Counter - Big Data Streaming Pipeline

A real-time streaming analytics system that processes social media data (Facebook and Twitter) to count hashtag occurrences using Apache Kafka, Apache Flink, and Docker.

## 🚀 Quick Start

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
docker logs -f flink-taskmanager1 | grep "🏷️"

# Check TaskManager 2 logs  
docker logs -f flink-taskmanager2 | grep "🏷️"
```

## 📊 Expected Output

You should see real-time hashtag counts like:
```
🏷️ Facebook HASHTAG COUNTS> (socialmedia,2)
🏷️ Facebook HASHTAG COUNTS> (feedback,1)
🏷️ Twitter HASHTAG COUNTS> (trending,3)
🏷️ Twitter HASHTAG COUNTS> (politics,1)
```

## 🖥️ Web Interfaces

- **Flink Dashboard**: http://localhost:8081
- **Kafdrop (Kafka UI)**: http://localhost:9000

## ⚡ Features

- Real-time hashtag counting every 15 seconds
- Separate processing for Facebook and Twitter streams
- Fault-tolerant distributed processing
- Web-based monitoring and management
- Configurable streaming parameters

## 🛠️ Troubleshooting

If you don't see hashtag counts:
1. Ensure producer.py is running
2. Check Flink jobs are active in web UI
3. Verify Kafka topics have data in Kafdrop
4. Monitor both TaskManager logs
