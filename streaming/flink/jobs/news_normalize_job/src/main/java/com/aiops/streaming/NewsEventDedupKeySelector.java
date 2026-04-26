package com.aiops.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Locale;

final class NewsEventDedupKeySelector {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static String dedupKeyFromNewsEventJson(String newsEventJson) {
    try {
      JsonNode node = MAPPER.readTree(newsEventJson);
      String url = NewsEventNormalizer.textOrNull(node.get("source_url"));
      if (url != null && !url.isBlank()) {
        return "url:" + url;
      }
      String title = NewsEventNormalizer.textOrNull(node.get("title"));
      if (title != null && !title.isBlank()) {
        return "title:" + title.toLowerCase(Locale.ROOT);
      }
      String id = NewsEventNormalizer.textOrNull(node.get("event_id"));
      return "id:" + (id == null ? "unknown" : id);
    } catch (Exception e) {
      return "parse_error";
    }
  }
}
