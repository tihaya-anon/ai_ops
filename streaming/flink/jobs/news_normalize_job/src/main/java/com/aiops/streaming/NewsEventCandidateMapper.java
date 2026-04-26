package com.aiops.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.LinkedHashSet;

final class NewsEventCandidateMapper {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static String toEventCandidateJson(String newsEventJson) {
    try {
      JsonNode node = MAPPER.readTree(newsEventJson);

      String eventId = textOrNull(node.get("event_id"));
      if (eventId == null || eventId.isBlank()) {
        return null;
      }

      ObjectNode out = MAPPER.createObjectNode();
      out.put("schema_version", "1");
      out.put("event_type", "event_candidate");
      out.put("candidate_id", eventId);
      out.put("source_event_id", eventId);
      out.put("observed_at", textOrEmpty(node.get("observed_at")));
      out.put("title", textOrEmpty(node.get("title")));
      out.put("body", textOrEmpty(node.get("body")));
      out.put("source_url", textOrEmpty(node.get("source_url")));
      out.put("language", textOrEmpty(node.get("language")));

      LinkedHashSet<String> symbols = new LinkedHashSet<>();
      JsonNode symbolsNode = node.get("symbols");
      if (symbolsNode != null && symbolsNode.isArray()) {
        for (JsonNode sym : symbolsNode) {
          String s = sym == null ? null : sym.asText(null);
          if (s != null && !s.isBlank()) {
            symbols.add(s.trim());
          }
          if (symbols.size() >= 20) {
            break;
          }
        }
      }

      ArrayNode outSymbols = out.putArray("symbols");
      for (String s : symbols) {
        outSymbols.add(s);
      }

      ArrayNode entities = out.putArray("entities");
      // rule_v1: leave empty; fill when we introduce rule/model extraction

      ArrayNode topics = out.putArray("topics");
      for (String s : symbols) {
        topics.add(s);
      }

      ObjectNode quality = out.putObject("quality");
      quality.put("extraction_ok", true);
      quality.put("extractor", "rule_v1");

      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      return null;
    }
  }

  private static String textOrNull(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    String s = node.asText(null);
    return s == null ? null : s;
  }

  private static String textOrEmpty(JsonNode node) {
    String s = textOrNull(node);
    return s == null ? "" : s;
  }
}
