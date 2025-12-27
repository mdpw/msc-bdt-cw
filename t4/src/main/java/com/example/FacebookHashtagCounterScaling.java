package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.InputStream;
import java.time.Duration;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class FacebookHashtagCounterScaling {

    public static class HashtagExtractor implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private final ObjectMapper mapper = new ObjectMapper();
        private final Pattern hashtagPattern = Pattern.compile("#(\\w+)", Pattern.CASE_INSENSITIVE);
        
        // ENHANCED METRICS FOR STEP 4 COMPARISON
        private int totalMessages = 0;
        private int messagesWithHashtags = 0;
        private int totalHashtags = 0;
        private int parseErrors = 0;
        private long totalProcessingTimeMs = 0;
        private long minLatencyMs = Long.MAX_VALUE;
        private long maxLatencyMs = 0;
        
        // STEP 4 SPECIFIC METRICS
        private int partitionMessages = 0;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // START TIMING
            long startTime = System.currentTimeMillis();
            totalMessages++;
            partitionMessages++;
            
            try {
                JsonNode root = mapper.readTree(value);
                JsonNode data = root.path("data");
                
                // Extract hashtags from comment_text field
                JsonNode commentTextNode = data.path("comment_text");
                if (!commentTextNode.isMissingNode()) {
                    String commentText = commentTextNode.asText();
                    
                    // Find all hashtags in the text using regex
                    Matcher matcher = hashtagPattern.matcher(commentText);
                    int hashtagsInMessage = 0;
                    
                    while (matcher.find()) {
                        String hashtag = matcher.group(1).toLowerCase();
                        out.collect(new Tuple2<>(hashtag, 1));
                        hashtagsInMessage++;
                        totalHashtags++;
                    }
                    
                    if (hashtagsInMessage > 0) {
                        messagesWithHashtags++;
                    }
                }
                
            } catch (Exception e) {
                parseErrors++;
            }
            
            // END TIMING
            long endTime = System.currentTimeMillis();
            long latency = endTime - startTime;
            totalProcessingTimeMs += latency;
            
            if (latency < minLatencyMs) minLatencyMs = latency;
            if (latency > maxLatencyMs) maxLatencyMs = latency;
            
            // PRINT METRICS EVERY 100 MESSAGES WITH STEP 4 INDICATORS
            if (totalMessages % 100 == 0) {
                printMetrics();
            }
        }
        
        private void printMetrics() {
            // Calculate metrics
            double hashtagCoverage = (totalMessages > 0) ? (messagesWithHashtags * 100.0 / totalMessages) : 0;
            double avgHashtagsPerMessage = (totalMessages > 0) ? (totalHashtags / (double) totalMessages) : 0;
            double errorRate = (totalMessages > 0) ? (parseErrors * 100.0 / totalMessages) : 0;
            double avgLatencyMs = (totalMessages > 0) ? (totalProcessingTimeMs / (double) totalMessages) : 0;
            double throughputPerSec = (avgLatencyMs > 0) ? (1000.0 / avgLatencyMs) : 0;
            
            System.out.println("\n" + "=".repeat(60));
            System.out.println("FACEBOOK STEP 4 METRICS (Messages: " + totalMessages + ") ");
            System.out.println("SCALING EXPERIMENT: 2 Partitions + Parallelism");
            System.out.println("=".repeat(60));
            
            // ACCURACY
            System.out.println("ACCURACY:");
            System.out.printf("   Messages with hashtags: %d (%.1f%%)%n", messagesWithHashtags, hashtagCoverage);
            System.out.printf("   Total hashtags found: %d%n", totalHashtags);
            System.out.printf("   Avg hashtags per message: %.2f%n", avgHashtagsPerMessage);
            System.out.printf("   Parse errors: %d (%.2f%%)%n", parseErrors, errorRate);
            
            // PERFORMANCE
            System.out.println("PERFORMANCE:");
            System.out.printf("   Avg latency: %.2f ms%n", avgLatencyMs);
            System.out.printf("   Min latency: %d ms%n", (minLatencyMs != Long.MAX_VALUE) ? minLatencyMs : 0);
            System.out.printf("   Max latency: %d ms%n", maxLatencyMs);
            System.out.printf("   Throughput: %.0f msg/sec%n", throughputPerSec);
            
            // STEP 4 SPECIFIC
            System.out.println("SCALING METRICS:");
            System.out.printf("   Partition messages: %d%n", partitionMessages);
            System.out.printf("   Parallelism: ENABLED (2 partitions)%n");
            System.out.printf("   Watermarking: PARTITION-AWARE%n");
            
            System.out.println("=".repeat(60) + "\n");
        }
    }

    // Load configuration from YAML file
    private static JsonNode loadConfig() throws Exception {
        ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory());
        InputStream configStream = FacebookHashtagCounterScaling.class.getClassLoader().getResourceAsStream("config.yml");
        if (configStream == null) {
            throw new RuntimeException("config.yml not found in resources");
        }
        return yamlMapper.readTree(configStream);
    }

    public static void main(String[] args) throws Exception {
        // LOAD CONFIGURATION FROM FILE
        JsonNode config = loadConfig();
        
        // Read ALL values from config.yml (same as Step 3)
        String kafkaServers = config.path("kafka").path("container_servers").asText("kafka:29092");
        String facebookTopic = config.path("kafka").path("facebook_topic").asText("facebook-posts");
        String facebookGroup = config.path("kafka").path("consumer_groups").path("facebook").asText("facebook-counter");
        int windowSeconds = config.path("flink").path("window_seconds").asInt(15);  
        int watermarkDelaySeconds = config.path("flink").path("watermark_delay_seconds").asInt(20); 
        int parallelism = config.path("flink").path("parallelism").asInt(2); 

        System.out.println("=== FACEBOOK HASHTAG COUNTER STEP 4 CONFIGURATION ===");
        System.out.println("SCALING EXPERIMENT: 2 Partitions + Parallelism");
        System.out.println("Kafka Servers: " + kafkaServers);
        System.out.println("Facebook Topic: " + facebookTopic);
        System.out.println("Consumer Group: " + facebookGroup);
        System.out.println("Window Seconds: " + windowSeconds);
        System.out.println("Watermark Delay: " + watermarkDelaySeconds + "s");
        System.out.println("Parallelism: " + parallelism);
        System.out.println("Window Type: EVENT TIME (with watermarks)");
        System.out.println("======================================================");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // STEP 4 KEY CHANGE: FORCE PARALLELISM (from config)
        env.setParallelism(parallelism);

        KafkaSource<String> facebookSource = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaServers)
                .setTopics(facebookTopic)
                .setGroupId(facebookGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // Same watermark strategy as Step 3 (for fair comparison)
        DataStream<String> facebookStream = env.fromSource(
                facebookSource,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(watermarkDelaySeconds))
                        .withTimestampAssigner((element, ts) -> System.currentTimeMillis()),
                "FacebookSource"
        );

        // Using Event Time windows (same as Step 3)
        DataStream<Tuple2<String, Integer>> hashtagCounts = facebookStream
                .flatMap(new HashtagExtractor())
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(windowSeconds)))
                .sum(1);

        hashtagCounts.print("FACEBOOK_STEP4_HASHTAG_COUNTS");

        env.execute("Facebook Hashtag Counter - Step 4 Scaling");
    }
}