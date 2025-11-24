package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class TwitterHashtagCounter {

    public static class HashtagExtractor implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private final ObjectMapper mapper = new ObjectMapper();
        private final Pattern hashtagPattern = Pattern.compile("#(\\w+)", Pattern.CASE_INSENSITIVE);
        
        // SAME METRICS AS FACEBOOK (for fair comparison)
        private int totalMessages = 0;
        private int messagesWithHashtags = 0;
        private int totalHashtags = 0;
        private int parseErrors = 0;
        private long totalProcessingTimeMs = 0;
        private long minLatencyMs = Long.MAX_VALUE;
        private long maxLatencyMs = 0;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // START TIMING
            long startTime = System.currentTimeMillis();
            totalMessages++;
            
            try {
                JsonNode root = mapper.readTree(value);
                JsonNode data = root.path("data");
                
                // ONLY EXTRACT FROM DESCRIPTION TEXT (same approach as Facebook)
                JsonNode descriptionNode = data.path("description");
                if (!descriptionNode.isMissingNode()) {
                    String description = descriptionNode.asText();
                    
                    // Find all hashtags in the text using regex
                    Matcher matcher = hashtagPattern.matcher(description);
                    int hashtagsInMessage = 0;
                    
                    while (matcher.find()) {
                        String hashtag = matcher.group(1).toLowerCase();
                        out.collect(new Tuple2<>(hashtag, 1));  // Same as Facebook - no prefix
                        hashtagsInMessage++;
                        totalHashtags++;
                    }
                    
                    if (hashtagsInMessage > 0) {
                        messagesWithHashtags++;
                    }
                }
                
                // REMOVED PRODUCER HASHTAG PROCESSING FOR FAIR COMPARISON
                
            } catch (Exception e) {
                parseErrors++;
            }
            
            // END TIMING
            long endTime = System.currentTimeMillis();
            long latency = endTime - startTime;
            totalProcessingTimeMs += latency;
            
            if (latency < minLatencyMs) minLatencyMs = latency;
            if (latency > maxLatencyMs) maxLatencyMs = latency;
            
            // PRINT METRICS EVERY 100 MESSAGES
            if (totalMessages % 100 == 0) {
                printMetrics();
            }
        }
        
        private void printMetrics() {
            // IDENTICAL CALCULATION LOGIC AS FACEBOOK
            double hashtagCoverage = (totalMessages > 0) ? (messagesWithHashtags * 100.0 / totalMessages) : 0;
            double avgHashtagsPerMessage = (totalMessages > 0) ? (totalHashtags / (double) totalMessages) : 0;
            double errorRate = (totalMessages > 0) ? (parseErrors * 100.0 / totalMessages) : 0;
            double avgLatencyMs = (totalMessages > 0) ? (totalProcessingTimeMs / (double) totalMessages) : 0;
            double throughputPerSec = (avgLatencyMs > 0) ? (1000.0 / avgLatencyMs) : 0;
            
            System.out.println("\n" + "=".repeat(50));
            System.out.println("TWITTER METRICS (Messages: " + totalMessages + ")");
            System.out.println("=".repeat(50));
            
            // IDENTICAL FORMAT AS FACEBOOK
            System.out.println("ACCURACY:");
            System.out.printf("   Messages with hashtags: %d (%.1f%%)%n", messagesWithHashtags, hashtagCoverage);
            System.out.printf("   Total hashtags found: %d%n", totalHashtags);
            System.out.printf("   Avg hashtags per message: %.2f%n", avgHashtagsPerMessage);
            System.out.printf("   Parse errors: %d (%.2f%%)%n", parseErrors, errorRate);
            
            // IDENTICAL PERFORMANCE SECTION
            System.out.println("PERFORMANCE:");
            System.out.printf("   Avg latency: %.2f ms%n", avgLatencyMs);
            System.out.printf("   Min latency: %d ms%n", (minLatencyMs != Long.MAX_VALUE) ? minLatencyMs : 0);
            System.out.printf("   Max latency: %d ms%n", maxLatencyMs);
            System.out.printf("   Throughput: %.0f msg/sec%n", throughputPerSec);
            
            System.out.println("=".repeat(50) + "\n");
        }
    }

    // Load configuration from YAML file
    private static JsonNode loadConfig() throws Exception {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        InputStream configStream = TwitterHashtagCounter.class.getClassLoader().getResourceAsStream("config.yml");
        if (configStream == null) {
            throw new RuntimeException("config.yml not found in resources");
        }
        return yamlMapper.readTree(configStream);
    }

    public static void main(String[] args) throws Exception {
        // LOAD CONFIGURATION FROM FILE
        JsonNode config = loadConfig();
        
        // READ VALUES FROM YOUR CONFIG.YML
        String kafkaServers = config.path("kafka").path("container_servers").asText("kafka:29092");
        String twitterTopic = config.path("kafka").path("twitter_topic").asText("twitter-posts");
        String twitterGroup = config.path("kafka").path("consumer_groups").path("twitter").asText("twitter-counter");
        int windowSeconds = config.path("flink").path("window_seconds").asInt(15);
        int watermarkDelaySeconds = config.path("flink").path("watermark_delay_seconds").asInt(20);

        System.out.println("=== TWITTER HASHTAG COUNTER CONFIGURATION ===");
        System.out.println("Kafka Servers: " + kafkaServers);
        System.out.println("Twitter Topic: " + twitterTopic);
        System.out.println("Consumer Group: " + twitterGroup);
        System.out.println("Window Seconds: " + windowSeconds);
        System.out.println("Watermark Delay: " + watermarkDelaySeconds);
        System.out.println("===============================================");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> twitterSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics(twitterTopic)
                .setGroupId(twitterGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> twitterStream = env.fromSource(
                twitterSource,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkDelaySeconds))
                        .withTimestampAssigner((element, ts) -> System.currentTimeMillis()),
                "TwitterSource"
        );

        DataStream<Tuple2<String, Integer>> hashtagCounts = twitterStream
                .flatMap(new HashtagExtractor())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(windowSeconds)))
                .sum(1);

        hashtagCounts.print("TWITTER_HASHTAG_COUNTS");

        env.execute("Twitter Hashtag Counter");
    }
}