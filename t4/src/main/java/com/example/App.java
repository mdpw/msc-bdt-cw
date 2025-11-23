package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.time.Duration;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.InputStream;
import java.util.Map;

public class App {
    
    // Configuration class to hold config values
    public static class Config {
        public String kafkaServers;
        public String twitterTopic;
        public String facebookTopic;
        public int windowSeconds;
        public int watermarkDelaySeconds;
        public int parallelism;
        public String jobName;
        
        public Config(Map<String, Object> configMap) {
            Map<String, Object> kafka = (Map<String, Object>) configMap.get("kafka");
            Map<String, Object> flink = (Map<String, Object>) configMap.get("flink");
            
            this.kafkaServers = (String) kafka.get("servers");
            this.twitterTopic = (String) kafka.get("twitter_topic");
            this.facebookTopic = (String) kafka.get("facebook_topic");
            
            this.windowSeconds = (Integer) flink.get("window_seconds");
            this.watermarkDelaySeconds = (Integer) flink.get("watermark_delay_seconds");
            this.parallelism = (Integer) flink.get("parallelism");
            this.jobName = (String) flink.get("job_name");
        }
    }
    
    // Function to extract hashtags from CSV data
    public static class HashtagExtractor implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final Pattern HASHTAG_PATTERN = Pattern.compile("#\\w+", Pattern.CASE_INSENSITIVE);
        
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // Extract hashtags from the CSV row
            Matcher matcher = HASHTAG_PATTERN.matcher(value);
            while (matcher.find()) {
                String hashtag = matcher.group().toLowerCase();
                out.collect(new Tuple2<>(hashtag, 1));
            }
        }
    }
    
    // Load configuration from config.yml
    private static Config loadConfig() throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        
        // Try to load from classpath (src/main/resources)
        InputStream configStream = App.class.getClassLoader().getResourceAsStream("config.yml");
        if (configStream == null) {
            throw new RuntimeException("config.yml file not found in classpath");
        }
        
        Map<String, Object> configMap = mapper.readValue(configStream, Map.class);
        return new Config(configMap);
    }
    
    public static void main(String[] args) throws Exception {
        
        // Load configuration
        Config config = loadConfig();
        System.out.println("Loaded configuration:");
        System.out.println("Kafka Servers: " + config.kafkaServers);
        System.out.println("Twitter Topic: " + config.twitterTopic);
        System.out.println("Facebook Topic: " + config.facebookTopic);
        System.out.println("Window Size: " + config.windowSeconds + " seconds");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Set parallelism from config
        env.setParallelism(config.parallelism);

        // Kafka source for Twitter topic
        KafkaSource<String> twitterSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")  // Docker internal address
                .setTopics(config.twitterTopic)
                .setGroupId("twitter-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Kafka source for Facebook topic
        KafkaSource<String> facebookSource = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:29092")  // Docker internal address
                .setTopics(config.facebookTopic)
                .setGroupId("facebook-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Create streams with watermarks for handling out-of-order events
        DataStream<String> twitterStream = env.fromSource(
                twitterSource,
                WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(config.watermarkDelaySeconds))
                        .withTimestampAssigner((element, ts) -> System.currentTimeMillis()),
                "TwitterSource"
        );

        DataStream<String> facebookStream = env.fromSource(
                facebookSource,
                WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(config.watermarkDelaySeconds))
                        .withTimestampAssigner((element, ts) -> System.currentTimeMillis()),
                "FacebookSource"
        );

        // Merge both streams
        DataStream<String> socialStream = twitterStream.union(facebookStream);
        
        // Extract hashtags and count them in configurable windows
        DataStream<Tuple2<String, Integer>> hashtagCounts = socialStream
                .flatMap(new HashtagExtractor())
                .keyBy(0)  // Group by hashtag
                .timeWindow(Time.seconds(config.windowSeconds))  // Configurable window size (15 seconds from config)
                .sum(1);   // Sum the counts

        // Print hashtag counts
        hashtagCounts.print("Hashtag Counts");

        // Also print raw data for debugging
        socialStream.print("Raw Posts");

        env.execute(config.jobName);
    }
}