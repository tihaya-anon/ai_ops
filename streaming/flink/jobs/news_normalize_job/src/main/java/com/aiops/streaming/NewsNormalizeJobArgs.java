package com.aiops.streaming;

public final class NewsNormalizeJobArgs {
  final String bootstrapServers;
  final String inputTopic;
  final String outputTopic;
  final String candidatesTopic;
  final String consumerGroupId;
  final int dedupTtlMinutes;
  final boolean pgEnabled;
  final String pgJdbcUrl;
  final String pgUser;
  final String pgPassword;
  final String pgTable;

  private NewsNormalizeJobArgs(
      String bootstrapServers,
      String inputTopic,
      String outputTopic,
      String candidatesTopic,
      String consumerGroupId,
      int dedupTtlMinutes,
      boolean pgEnabled,
      String pgJdbcUrl,
      String pgUser,
      String pgPassword,
      String pgTable) {
    this.bootstrapServers = bootstrapServers;
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
    this.candidatesTopic = candidatesTopic;
    this.consumerGroupId = consumerGroupId;
    this.dedupTtlMinutes = dedupTtlMinutes;
    this.pgEnabled = pgEnabled;
    this.pgJdbcUrl = pgJdbcUrl;
    this.pgUser = pgUser;
    this.pgPassword = pgPassword;
    this.pgTable = pgTable;
  }

  static NewsNormalizeJobArgs parse(String[] args) {
    String bootstrap = envOrDefault("KAFKA_BOOTSTRAP", "redpanda:9092");
    String input = envOrDefault("INPUT_TOPIC", "news_events_raw_v1");
    String output = envOrDefault("OUTPUT_TOPIC", "news_events_v1");
    String candidates = envOrDefault("CANDIDATES_TOPIC", "event_candidates_v1");
    String group = envOrDefault("CONSUMER_GROUP", "news_normalize_job_v1");
    int ttl = Integer.parseInt(envOrDefault("DEDUP_TTL_MINUTES", "60"));
    boolean pgEnabled = parseBoolean(envOrDefault("PG_ENABLED", "false"));
    String pgJdbcUrl = envOrDefault("PG_JDBC_URL", "");
    String pgUser = envOrDefault("PG_USER", "");
    String pgPassword = envOrDefault("PG_PASSWORD", "");
    String pgTable = envOrDefault("PG_TABLE", "event_candidates_v1");

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
        case "--candidates-topic":
          candidates = args[++i];
          break;
        case "--group-id":
          group = args[++i];
          break;
        case "--dedup-ttl-minutes":
          ttl = Integer.parseInt(args[++i]);
          break;
        case "--pg-enabled":
          pgEnabled = parseBoolean(args[++i]);
          break;
        case "--pg-jdbc-url":
          pgJdbcUrl = args[++i];
          break;
        case "--pg-user":
          pgUser = args[++i];
          break;
        case "--pg-password":
          pgPassword = args[++i];
          break;
        case "--pg-table":
          pgTable = args[++i];
          break;
        default:
          break;
      }
    }

    return new NewsNormalizeJobArgs(
        bootstrap,
        input,
        output,
        candidates,
        group,
        ttl,
        pgEnabled,
        pgJdbcUrl,
        pgUser,
        pgPassword,
        pgTable);
  }

  private static String envOrDefault(String name, String defaultValue) {
    String value = System.getenv(name);
    return value == null || value.isBlank() ? defaultValue : value;
  }

  private static boolean parseBoolean(String value) {
    if (value == null) {
      return false;
    }
    String v = value.trim().toLowerCase(java.util.Locale.ROOT);
    return v.equals("1") || v.equals("true") || v.equals("yes") || v.equals("y") || v.equals("on");
  }
}
