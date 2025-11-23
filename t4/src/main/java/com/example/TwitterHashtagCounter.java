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

import java.time.Duration;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class TwitterHashtagCounter {

    public static class HashtagExtractor implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private final ObjectMapper mapper = new ObjectMapper();
        private final Pattern hashtagPattern = Pattern.compile("#(\\w+)", Pattern.CASE_INSENSITIVE);

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            try {
                JsonNode root = mapper.readTree(value);
                JsonNode data = root.path("data");
                
                // Extract hashtags from description field
                JsonNode descriptionNode = data.path("description");
                if (!descriptionNode.isMissingNode()) {
                    String description = descriptionNode.asText();
                    
                    // Find all hashtags in the text using regex
                    Matcher matcher = hashtagPattern.matcher(description);
                    while (matcher.find()) {
                        String hashtag = matcher.group(1).toLowerCase(); // Get hashtag without #
                        out.collect(new Tuple2<>(hashtag, 1));
                    }
                }
            } catch (Exception e) {
                // ignore invalid JSON
            }
        }
    }

    public static void main(String[] args) throws Exception {
        String kafkaServers = "kafka:29092";
        String twitterTopic = "twitter-posts";
        String twitterGroup = "twitter-counter";
        int windowSeconds = 15;
        int watermarkDelaySeconds = 5;

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

        hashtagCounts.print("🏷️ Twitter HASHTAG COUNTS");

        env.execute("Twitter Hashtag Counter");
    }
}