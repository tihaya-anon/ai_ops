package com.aiops.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public final class NewsNormalizeJob {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final DateTimeFormatter[] TIME_FORMATS =
      new DateTimeFormatter[] {
        DateTimeFormatter.ISO_OFFSET_DATE_TIME,
        DateTimeFormatter.RFC_1123_DATE_TIME,
        DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").withZone(ZoneOffset.UTC),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC),
      };

  public static void main(String[] args) throws Exception {
    JobArgs jobArgs = JobArgs.parse(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    KafkaSource<String> source =
        KafkaSource.<String>builder()
            .setBootstrapServers(jobArgs.bootstrapServers)
            .setTopics(jobArgs.inputTopic)
            .setGroupId(jobArgs.consumerGroupId)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
            .setProperties(consumerProps)
            .build();

    DataStream<String> raw =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_news_raw");

    DataStream<String> normalized =
        raw.map(NewsNormalizeJob::normalizeEnvelopeJson, TypeInformation.of(String.class))
            .name("normalize")
            .filter(Objects::nonNull)
            .name("drop_invalid")
            .keyBy(NewsNormalizeJob::dedupKeyFromNewsEventJson)
            .process(new DedupWithinTtl(jobArgs.dedupTtlMinutes))
            .name("dedup_ttl");

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "50");

    KafkaSink<String> sink =
        KafkaSink.<String>builder()
            .setBootstrapServers(jobArgs.bootstrapServers)
            .setKafkaProducerConfig(producerProps)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                    .setTopic(jobArgs.outputTopic)
                    .setValueSerializationSchema(
                        new org.apache.flink.api.common.serialization.SimpleStringSchema())
                    .build())
            .build();

    normalized.sinkTo(sink).name("kafka_news_events_v1");

    env.execute("news_normalize_job");
  }

  private static String normalizeEnvelopeJson(String envelopeJson) {
    try {
      JsonNode envelope = MAPPER.readTree(envelopeJson);
      JsonNode payload = envelope.get("payload");
      if (payload == null || !payload.isObject()) {
        return null;
      }

      String fetchedAt = textOrNull(envelope.get("fetched_at"));
      String providerMessageId = textOrNull(payload.get("provider_message_id"));
      String url = textOrNull(payload.get("url"));

      String title = textOrEmpty(payload.get("title"));
      String body = textOrEmpty(payload.get("body"));
      body = stripHtml(body);
      body = collapseWhitespace(body);

      boolean parsedTimeOk = true;
      Instant observedAt = parsePublishedAt(payload.get("published_at")).orElse(null);
      if (observedAt == null) {
        observedAt = parseTimeString(fetchedAt).orElse(null);
        parsedTimeOk = false;
      }
      if (observedAt == null) {
        observedAt = Instant.now();
        parsedTimeOk = false;
      }

      String normalizedUrl = normalizeUrl(url);
      String language = normalizeLanguage(textOrNull(payload.get("language")));
      List<String> symbols = normalizeSymbols(payload.get("symbols"));

      String stableIdInput = providerMessageId != null ? providerMessageId : (normalizedUrl != null ? normalizedUrl : title);
      String eventId = stableId(stableIdInput);

      ObjectNode out = MAPPER.createObjectNode();
      out.put("schema_version", "1");
      out.put("event_type", "news_event");
      out.put("event_id", eventId);
      out.put("observed_at", observedAt.atOffset(ZoneOffset.UTC).toString().replace("+00:00", "Z"));
      out.put("title", title);
      out.put("body", body);
      out.put("source_url", normalizedUrl == null ? "" : normalizedUrl);
      out.put("language", language);

      ArrayNode sym = out.putArray("symbols");
      for (String s : symbols) {
        sym.add(s);
      }

      out.put("provider", textOrNull(payload.get("provider")));
      out.put("channel", textOrNull(payload.get("channel")));

      ObjectNode quality = out.putObject("quality");
      quality.put("parsed_time_ok", parsedTimeOk);
      quality.putNull("is_duplicate");

      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      return null;
    }
  }

  private static String dedupKeyFromNewsEventJson(String newsEventJson) {
    try {
      JsonNode node = MAPPER.readTree(newsEventJson);
      String url = textOrNull(node.get("source_url"));
      if (url != null && !url.isBlank()) {
        return "url:" + url;
      }
      String title = textOrNull(node.get("title"));
      if (title != null && !title.isBlank()) {
        return "title:" + title.toLowerCase(Locale.ROOT);
      }
      String id = textOrNull(node.get("event_id"));
      return "id:" + (id == null ? "unknown" : id);
    } catch (Exception e) {
      return "parse_error";
    }
  }

  private static final class DedupWithinTtl extends KeyedProcessFunction<String, String, String> {
    private final int ttlMinutes;
    private transient ValueState<Boolean> seen;

    DedupWithinTtl(int ttlMinutes) {
      this.ttlMinutes = ttlMinutes;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
      StateTtlConfig ttl =
          StateTtlConfig.newBuilder(Duration.ofMinutes(ttlMinutes))
              .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
              .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
              .build();
      ValueStateDescriptor<Boolean> desc =
          new ValueStateDescriptor<>("seen", TypeInformation.of(Boolean.class));
      desc.enableTimeToLive(ttl);
      seen = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
      Boolean already = seen.value();
      if (already != null && already) {
        return;
      }
      seen.update(true);
      out.collect(value);
    }
  }

  private static Optional<Instant> parsePublishedAt(JsonNode publishedAt) {
    if (publishedAt == null || publishedAt.isNull()) {
      return Optional.empty();
    }
    if (publishedAt.isNumber()) {
      long v = publishedAt.asLong();
      if (v > 1_000_000_000_000L) {
        return Optional.of(Instant.ofEpochMilli(v));
      }
      return Optional.of(Instant.ofEpochSecond(v));
    }
    if (publishedAt.isTextual()) {
      return parseTimeString(publishedAt.asText());
    }
    return Optional.empty();
  }

  private static Optional<Instant> parseTimeString(String s) {
    if (s == null) {
      return Optional.empty();
    }
    String t = s.trim();
    if (t.isEmpty()) {
      return Optional.empty();
    }
    if (t.matches("^\\d{10,13}$")) {
      long v = Long.parseLong(t);
      if (t.length() >= 13) {
        return Optional.of(Instant.ofEpochMilli(v));
      }
      return Optional.of(Instant.ofEpochSecond(v));
    }
    for (DateTimeFormatter fmt : TIME_FORMATS) {
      try {
        if (fmt == DateTimeFormatter.ISO_OFFSET_DATE_TIME || fmt == DateTimeFormatter.RFC_1123_DATE_TIME) {
          return Optional.of(OffsetDateTime.parse(t, fmt).toInstant());
        }
        return Optional.of(Instant.from(fmt.parse(t)));
      } catch (DateTimeParseException ignored) {
      }
    }
    try {
      return Optional.of(Instant.parse(t));
    } catch (DateTimeParseException ignored) {
      return Optional.empty();
    }
  }

  private static String normalizeLanguage(String lang) {
    if (lang == null) {
      return "und";
    }
    String l = lang.trim().toLowerCase(Locale.ROOT);
    if (l.equals("en") || l.startsWith("en-")) {
      return "en";
    }
    if (l.equals("zh") || l.startsWith("zh-") || l.equals("cn")) {
      return "zh";
    }
    return "und";
  }

  private static List<String> normalizeSymbols(JsonNode symbolsNode) {
    LinkedHashSet<String> out = new LinkedHashSet<>();
    if (symbolsNode == null || symbolsNode.isNull()) {
      return List.of();
    }
    if (symbolsNode.isArray()) {
      for (JsonNode n : symbolsNode) {
        if (n == null || n.isNull()) {
          continue;
        }
        String s = n.asText(null);
        if (s == null) {
          continue;
        }
        String trimmed = s.trim();
        if (!trimmed.isEmpty()) {
          out.add(trimmed);
        }
        if (out.size() >= 20) {
          break;
        }
      }
    } else {
      String s = symbolsNode.asText(null);
      if (s != null && !s.trim().isEmpty()) {
        out.add(s.trim());
      }
    }
    return new ArrayList<>(out);
  }

  private static String normalizeUrl(String url) {
    if (url == null) {
      return null;
    }
    String u = url.trim();
    if (u.isEmpty()) {
      return null;
    }
    if (u.startsWith("http://") || u.startsWith("https://")) {
      return u;
    }
    return null;
  }

  private static String stripHtml(String s) {
    if (s == null || s.isEmpty()) {
      return "";
    }
    return s.replaceAll("<[^>]+>", " ");
  }

  private static String collapseWhitespace(String s) {
    if (s == null || s.isEmpty()) {
      return "";
    }
    return s.replaceAll("\\s+", " ").trim();
  }

  private static String textOrNull(JsonNode n) {
    if (n == null || n.isNull()) {
      return null;
    }
    if (!n.isTextual()) {
      return n.asText(null);
    }
    String s = n.asText();
    return s == null ? null : s;
  }

  private static String textOrEmpty(JsonNode n) {
    String s = textOrNull(n);
    return s == null ? "" : s;
  }

  private static String stableId(String input) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      byte[] hash = md.digest((input == null ? "" : input).getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 16 && i < hash.length; i++) {
        sb.append(String.format("%02x", hash[i]));
      }
      return sb.toString();
    } catch (Exception e) {
      return String.valueOf(Objects.hashCode(input));
    }
  }

  private static final class JobArgs {
    final String bootstrapServers;
    final String inputTopic;
    final String outputTopic;
    final String consumerGroupId;
    final int dedupTtlMinutes;

    private JobArgs(
        String bootstrapServers,
        String inputTopic,
        String outputTopic,
        String consumerGroupId,
        int dedupTtlMinutes) {
      this.bootstrapServers = bootstrapServers;
      this.inputTopic = inputTopic;
      this.outputTopic = outputTopic;
      this.consumerGroupId = consumerGroupId;
      this.dedupTtlMinutes = dedupTtlMinutes;
    }

    static JobArgs parse(String[] args) {
      String bootstrap = envOrDefault("KAFKA_BOOTSTRAP", "redpanda:9092");
      String in = envOrDefault("INPUT_TOPIC", "news_events_raw_v1");
      String out = envOrDefault("OUTPUT_TOPIC", "news_events_v1");
      String group = envOrDefault("CONSUMER_GROUP", "news_normalize_job_v1");
      int ttl = Integer.parseInt(envOrDefault("DEDUP_TTL_MINUTES", "60"));

      for (int i = 0; i < args.length; i++) {
        switch (args[i]) {
          case "--bootstrap":
            bootstrap = args[++i];
            break;
          case "--input-topic":
            in = args[++i];
            break;
          case "--output-topic":
            out = args[++i];
            break;
          case "--group-id":
            group = args[++i];
            break;
          case "--dedup-ttl-minutes":
            ttl = Integer.parseInt(args[++i]);
            break;
          default:
            break;
        }
      }

      return new JobArgs(bootstrap, in, out, group, ttl);
    }

    private static String envOrDefault(String name, String def) {
      String v = System.getenv(name);
      return v == null || v.isBlank() ? def : v;
    }
  }
}
