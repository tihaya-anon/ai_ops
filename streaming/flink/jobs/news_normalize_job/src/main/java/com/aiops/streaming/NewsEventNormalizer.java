package com.aiops.streaming;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
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

final class NewsEventNormalizer {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final DateTimeFormatter[] TIME_FORMATS = new DateTimeFormatter[] {
      DateTimeFormatter.ISO_OFFSET_DATE_TIME,
      DateTimeFormatter.RFC_1123_DATE_TIME,
      DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss").withZone(ZoneOffset.UTC),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC),
  };

  static String normalizeEnvelopeJson(String envelopeJson) {
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
      String body = normalizeBody(textOrEmpty(payload.get("body")));

      ObservedAt observed = determineObservedAt(payload.get("published_at"), fetchedAt);

      String normalizedUrl = normalizeUrl(url);
      String language = normalizeLanguage(textOrNull(payload.get("language")));
      List<String> symbols = normalizeSymbols(payload.get("symbols"));

      String stableIdInput = providerMessageId != null ? providerMessageId
          : (normalizedUrl != null ? normalizedUrl : title);
      String eventId = stableId(stableIdInput);

      ObjectNode out = MAPPER.createObjectNode();
      out.put("schema_version", "1");
      out.put("event_type", "news_event");
      out.put("event_id", eventId);
      out.put("observed_at", formatUtcInstant(observed.observedAt));
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
      quality.put("parsed_time_ok", observed.parsedTimeOk);
      quality.putNull("is_duplicate");

      return MAPPER.writeValueAsString(out);
    } catch (Exception e) {
      return null;
    }
  }

  static String textOrNull(JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    if (!node.isTextual()) {
      return node.asText(null);
    }
    String s = node.asText();
    return s == null ? null : s;
  }

  private static String textOrEmpty(JsonNode node) {
    String s = textOrNull(node);
    return s == null ? "" : s;
  }

  private static String normalizeBody(String body) {
    return collapseWhitespace(stripHtml(body));
  }

  private static ObservedAt determineObservedAt(JsonNode publishedAt, String fetchedAt) {
    Instant observedAt = parsePublishedAt(publishedAt).orElse(null);
    if (observedAt != null) {
      return new ObservedAt(observedAt, true);
    }

    observedAt = parseTimeString(fetchedAt).orElse(null);
    if (observedAt != null) {
      return new ObservedAt(observedAt, false);
    }

    return new ObservedAt(Instant.now(), false);
  }

  private static String formatUtcInstant(Instant instant) {
    return instant.atOffset(ZoneOffset.UTC).toString().replace("+00:00", "Z");
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

  private static final class ObservedAt {
    final Instant observedAt;
    final boolean parsedTimeOk;

    ObservedAt(Instant observedAt, boolean parsedTimeOk) {
      this.observedAt = observedAt;
      this.parsedTimeOk = parsedTimeOk;
    }
  }
}
