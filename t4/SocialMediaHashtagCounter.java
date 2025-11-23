import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Step 3: Social Media Hashtag Counter - Config-based Implementation
 * Reads from config.yml, processes Twitter and Facebook streams
 */
public class SocialMediaHashtagCounter {

    public static class Config {
        public String kafkaServers;
        public String twitterTopic;
        public String facebookTopic;
        public String twitterConsumerGroup;
        public String facebookConsumerGroup;
        public int windowSeconds;
        public int watermarkDelaySeconds;
        public int parallelism;
        public String jobName;
    }

    public static Config loadConfig(String configFile) throws Exception {
        Yaml yaml = new Yaml();
        Config config = new Config();

        try (InputStream inputStream = new FileInputStream(configFile)) {
            Map<String, Object> data = yaml.load(inputStream);

            Map<String, Object> kafka = (Map<String, Object>) data.get("kafka");
            config.kafkaServers = (String) kafka.get("servers");
            config.twitterTopic = (String) kafka.get("twitter_topic");
            config.facebookTopic = (String) kafka.get("facebook_topic");

            Map<String, Object> consumerGroups = (Map<String, Object>) kafka.get("consumer_groups");
            config.twitterConsumerGroup = (String) consumerGroups.get("twitter");
            config.facebookConsumerGroup = (String) consumerGroups.get("facebook");

            Map<String, Object> flink = (Map<String, Object>) data.get("flink");
            config.windowSeconds = (Integer) flink.get("window_seconds");
            config.watermarkDelaySeconds = (Integer) flink.get("watermark_delay_seconds");
            config.parallelism = (Integer) flink.get("parallelism");
            config.jobName = (String) flink.get("job_name");

            System.out.println("Config loaded: " + config.kafkaServers);
            return config;
        }
    }

    public static class HashtagCount {
        public String hashtag;
        public String source;
        public long count;
        public long windowStart;
        public long windowEnd;

        public HashtagCount(String hashtag, String source, long count, long windowStart, long windowEnd) {
            this.hashtag = hashtag;
            this.source = source;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return String.format("%s Hashtag Counts> {hashtag='%s', count=%d, window_start=%d, window_end=%d}",
                    source, hashtag, count, windowStart, windowEnd);
        }
    }

    public static class TwitterHashtagExtractor implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final Pattern hashtagPattern = Pattern.compile("#(\\w+)", Pattern.CASE_INSENSITIVE);

        @Override
        public void flatMap(String message, Collector<Tuple2<String, Integer>> out) throws Exception {
            try {
                JsonNode jsonNode = objectMapper.readTree(message);
                JsonNode dataNode = jsonNode.has("data") ? jsonNode.get("data") : jsonNode;

                // Extract from hashtags field
                JsonNode hashtagsNode = dataNode.get("hashtags");
                if (hashtagsNode != null && !hashtagsNode.isNull()) {
                    if (hashtagsNode.isArray()) {
                        for (JsonNode hashtagNode : hashtagsNode) {
                            String hashtag = hashtagNode.asText().trim();
                            if (!hashtag.isEmpty()) {
                                out.collect(new Tuple2<>("#" + hashtag.toLowerCase(), 1));
                            }
                        }
                    } else if (hashtagsNode.isTextual()) {
                        String hashtagText = hashtagsNode.asText();
                        Matcher matcher = hashtagPattern.matcher(hashtagText);
                        while (matcher.find()) {
                            out.collect(new Tuple2<>("#" + matcher.group(1).toLowerCase(), 1));
                        }
                    }
                }

                // Extract from description
                JsonNode descriptionNode = dataNode.get("description");
                if (descriptionNode != null && !descriptionNode.isNull()) {
                    String description = descriptionNode.asText();
                    Matcher matcher = hashtagPattern.matcher(description);
                    while (matcher.find()) {
                        out.collect(new Tuple2<>("#" + matcher.group(1).toLowerCase(), 1));
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing Twitter message: " + e.getMessage());
            }
        }
    }

    public static class FacebookHashtagExtractor implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private final ObjectMapper objectMapper = new ObjectMapper();
        private final Pattern hashtagPattern = Pattern.compile("#(\\w+)", Pattern.CASE_INSENSITIVE);

        @Override
        public void flatMap(String message, Collector<Tuple2<String, Integer>> out) throws Exception {
            try {
                JsonNode jsonNode = objectMapper.readTree(message);
                JsonNode dataNode = jsonNode.has("data") ? jsonNode.get("data") : jsonNode;

                JsonNode commentTextNode = dataNode.get("comment_text");
                if (commentTextNode != null && !commentTextNode.isNull()) {
                    String commentText = commentTextNode.asText();
                    Matcher matcher = hashtagPattern.matcher(commentText);
                    while (matcher.find()) {
                        out.collect(new Tuple2<>("#" + matcher.group(1).toLowerCase(), 1));
                    }
                }
            } catch (Exception e) {
                System.err.println("Error processing Facebook message: " + e.getMessage());
            }
        }
    }

    public static class HashtagCountWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, HashtagCount, String, TimeWindow> {
        private final String source;

        public HashtagCountWindowFunction(String source) {
            this.source = source;
        }

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, 
                          Collector<HashtagCount> out) throws Exception {
            long count = 0;
            for (Tuple2<String, Integer> element : elements) {
                count += element.f1;
            }
            out.collect(new HashtagCount(key, source, count, context.window().getStart(), context.window().getEnd()));
        }
    }

    public static DataStream<HashtagCount> createProcessingPipeline(
            StreamExecutionEnvironment env, Config config, String sourceName, String topic, 
            String consumerGroup, FlatMapFunction<String, Tuple2<String, Integer>> extractorFunction) {

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(config.kafkaServers)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        WatermarkStrategy<String> watermarkStrategy = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(Duration.ofSeconds(config.watermarkDelaySeconds))
                .withTimestampAssigner((event, timestamp) -> System.currentTimeMillis());

        return env.fromSource(kafkaSource, watermarkStrategy, sourceName + "-source")
                .flatMap(extractorFunction)
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(config.windowSeconds)))
                .process(new HashtagCountWindowFunction(sourceName));
    }

    public static void main(String[] args) throws Exception {
        System.out.println("=".repeat(60));
        System.out.println("Step 3: Social Media Hashtag Counter");
        System.out.println("=".repeat(60));

        Config config = loadConfig("config.yml");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism);

        DataStream<HashtagCount> twitterResults = createProcessingPipeline(
                env, config, "Twitter", config.twitterTopic, config.twitterConsumerGroup, new TwitterHashtagExtractor());

        DataStream<HashtagCount> facebookResults = createProcessingPipeline(
                env, config, "Facebook", config.facebookTopic, config.facebookConsumerGroup, new FacebookHashtagExtractor());

        twitterResults.print();
        facebookResults.print();
        twitterResults.union(facebookResults).print();

        System.out.println("Starting Flink job: " + config.jobName);
        System.out.println("Window size: " + config.windowSeconds + "s, Watermark delay: " + config.watermarkDelaySeconds + "s");
        System.out.println("Press Ctrl+C to stop");

        env.execute(config.jobName);
    }
}