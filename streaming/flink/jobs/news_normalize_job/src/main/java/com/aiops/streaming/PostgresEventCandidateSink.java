package com.aiops.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

final class PostgresEventCandidateSink extends RichSinkFunction<String> {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int DEFAULT_BATCH_SIZE = 50;

  private final String jdbcUrl;
  private final String user;
  private final String password;
  private final String table;
  private final int batchSize;

  private transient Connection connection;
  private transient PreparedStatement upsert;
  private transient List<Row> buffer;

  PostgresEventCandidateSink(String jdbcUrl, String user, String password, String table, int batchSize) {
    this.jdbcUrl = Objects.requireNonNull(jdbcUrl, "jdbcUrl");
    this.user = user;
    this.password = password;
    this.table = Objects.requireNonNull(table, "table");
    this.batchSize = batchSize > 0 ? batchSize : DEFAULT_BATCH_SIZE;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    if (jdbcUrl.isBlank()) {
      throw new IllegalArgumentException("PG enabled but PG_JDBC_URL/--pg-jdbc-url is empty");
    }
    String validatedTable = validateTableName(table);

    Properties props = new Properties();
    if (user != null && !user.isBlank()) {
      props.put("user", user);
    }
    if (password != null && !password.isBlank()) {
      props.put("password", password);
    }

    connection = DriverManager.getConnection(jdbcUrl, props);
    connection.setAutoCommit(false);

    String sql = ""
        + "INSERT INTO " + validatedTable + " (candidate_id, source_event_id, observed_at, payload) "
        + "VALUES (?, ?, ?::timestamptz, ?::jsonb) "
        + "ON CONFLICT (candidate_id) DO UPDATE SET "
        + "source_event_id = EXCLUDED.source_event_id, "
        + "observed_at = EXCLUDED.observed_at, "
        + "payload = EXCLUDED.payload, "
        + "updated_at = now()";

    upsert = connection.prepareStatement(sql);
    buffer = new ArrayList<>(batchSize);
  }

  @Override
  public void invoke(String value, Context context) throws Exception {
    Row row = parse(value);
    if (row == null) {
      return;
    }

    buffer.add(row);
    if (buffer.size() >= batchSize) {
      flush();
    }
  }

  @Override
  public void close() throws Exception {
    try {
      flush();
    } finally {
      if (upsert != null) {
        upsert.close();
      }
      if (connection != null) {
        connection.close();
      }
    }
  }

  private void flush() throws SQLException {
    if (buffer == null || buffer.isEmpty()) {
      return;
    }

    for (Row row : buffer) {
      upsert.setString(1, row.candidateId);
      upsert.setString(2, row.sourceEventId);
      if (row.observedAt == null || row.observedAt.isBlank()) {
        upsert.setObject(3, null);
      } else {
        upsert.setString(3, row.observedAt);
      }
      upsert.setString(4, row.payloadJson);
      upsert.addBatch();
    }

    upsert.executeBatch();
    connection.commit();
    buffer.clear();
  }

  private static Row parse(String candidateJson) {
    try {
      JsonNode node = MAPPER.readTree(candidateJson);
      String candidateId = asText(node.get("candidate_id"));
      String sourceEventId = asText(node.get("source_event_id"));
      if (candidateId == null || candidateId.isBlank()) {
        return null;
      }
      if (sourceEventId == null || sourceEventId.isBlank()) {
        sourceEventId = candidateId;
      }
      String observedAt = asText(node.get("observed_at"));
      return new Row(candidateId, sourceEventId, observedAt, candidateJson);
    } catch (Exception e) {
      return null;
    }
  }

  private static String asText(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    return node.asText(null);
  }

  private static String validateTableName(String raw) {
    String t = raw.trim();
    if (t.isEmpty()) {
      throw new IllegalArgumentException("PG table is empty");
    }
    String lowered = t.toLowerCase(Locale.ROOT);
    if (!lowered.matches("^[a-z0-9_\\.]+$")) {
      throw new IllegalArgumentException("Invalid PG table name: " + raw);
    }
    return t;
  }

  private static final class Row {
    final String candidateId;
    final String sourceEventId;
    final String observedAt;
    final String payloadJson;

    Row(String candidateId, String sourceEventId, String observedAt, String payloadJson) {
      this.candidateId = candidateId;
      this.sourceEventId = sourceEventId;
      this.observedAt = observedAt;
      this.payloadJson = payloadJson;
    }
  }
}
