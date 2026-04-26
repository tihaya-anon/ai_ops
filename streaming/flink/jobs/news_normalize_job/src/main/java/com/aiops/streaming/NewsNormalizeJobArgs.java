package com.aiops.streaming;

public final class NewsNormalizeJobArgs {
  final String bootstrapServers;
  final String inputTopic;
  final String outputTopic;
  final String consumerGroupId;
  final int dedupTtlMinutes;

  private NewsNormalizeJobArgs(
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

  static NewsNormalizeJobArgs parse(String[] args) {
    String bootstrap = envOrDefault("KAFKA_BOOTSTRAP", "redpanda:9092");
    String input = envOrDefault("INPUT_TOPIC", "news_events_raw_v1");
    String output = envOrDefault("OUTPUT_TOPIC", "news_events_v1");
    String group = envOrDefault("CONSUMER_GROUP", "news_normalize_job_v1");
    int ttl = Integer.parseInt(envOrDefault("DEDUP_TTL_MINUTES", "60"));

    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--bootstrap":
          bootstrap = args[++i];
          break;
        case "--input-topic":
          input = args[++i];
          break;
        case "--output-topic":
          output = args[++i];
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

    return new NewsNormalizeJobArgs(bootstrap, input, output, group, ttl);
  }

  private static String envOrDefault(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }
}
