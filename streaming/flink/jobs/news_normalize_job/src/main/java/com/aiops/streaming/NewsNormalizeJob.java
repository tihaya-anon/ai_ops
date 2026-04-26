package com.aiops.streaming;

import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public final class NewsNormalizeJob {
  public static void main(String[] args) throws Exception {
    NewsNormalizeJobArgs jobArgs = NewsNormalizeJobArgs.parse(args);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    configureEnvironment(env);

    KafkaSource<String> source = buildSource(jobArgs);
    DataStream<String> normalized = buildNormalizedStream(env, source, jobArgs);

    KafkaSink<String> sink = buildSink(jobArgs);
    normalized.sinkTo(sink).name("kafka_news_events_v1");

    env.execute("news_normalize_job");
  }

  private static void configureEnvironment(StreamExecutionEnvironment env) {
    env.getConfig().setAutoWatermarkInterval(1000L);
  }

  private static KafkaSource<String> buildSource(NewsNormalizeJobArgs jobArgs) {
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    return KafkaSource.<String>builder()
        .setBootstrapServers(jobArgs.bootstrapServers)
        .setTopics(jobArgs.inputTopic)
        .setGroupId(jobArgs.consumerGroupId)
        .setStartingOffsets(OffsetsInitializer.latest())
        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
        .setProperties(consumerProps)
        .build();
  }

  private static DataStream<String> buildNormalizedStream(
      StreamExecutionEnvironment env,
      KafkaSource<String> source,
      NewsNormalizeJobArgs jobArgs) {
    DataStream<String> raw = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_news_raw");

    return raw.map(NewsEventNormalizer::normalizeEnvelopeJson, TypeInformation.of(String.class))
        .name("normalize")
        .filter(value -> value != null)
        .name("drop_invalid")
        .keyBy(NewsEventDedupKeySelector::dedupKeyFromNewsEventJson)
        .process(new DedupWithinTtlProcessFunction(jobArgs.dedupTtlMinutes))
        .name("dedup_ttl");
  }

  private static KafkaSink<String> buildSink(NewsNormalizeJobArgs jobArgs) {
    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, "50");

    return KafkaSink.<String>builder()
        .setBootstrapServers(jobArgs.bootstrapServers)
        .setKafkaProducerConfig(producerProps)
        .setRecordSerializer(
            KafkaRecordSerializationSchema.builder()
                .setTopic(jobArgs.outputTopic)
                .setValueSerializationSchema(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build())
        .build();
  }
}
