from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
from kafka import KafkaProducer


def _utc_iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value is not None and value != "" else default


def _normalize_bootstrap(bootstrap: str) -> list[str]:
    parts = [p.strip() for p in bootstrap.split(",")]
    return [p for p in parts if p]


def _build_envelope(
    *,
    raw_item: dict[str, Any],
    source_system: str,
    fetched_at: str,
    ingested_at: str,
    run_id: str,
    request_id: str,
) -> dict[str, Any]:
    source: dict[str, Any] = {
        "provider": raw_item.get("provider"),
        "channel": raw_item.get("channel"),
    }
    return {
        "schema_version": "1",
        "event_type": "news_raw",
        "event_id": str(uuid.uuid4()),
        "source_system": source_system,
        "source": source,
        "fetched_at": fetched_at,
        "ingested_at": ingested_at,
        "trace": {"run_id": run_id, "request_id": request_id},
        "payload": raw_item,
    }


def _message_key(envelope: dict[str, Any]) -> bytes:
    payload = envelope.get("payload") or {}
    key = payload.get("provider_message_id") or payload.get("url") or envelope.get("event_id")
    return str(key).encode("utf-8")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Fetch dirty news messages and publish to Kafka (pre-Flink).")
    parser.add_argument("--api-base-url", default=_env("API_BASE_URL", "http://localhost:8000"))
    parser.add_argument("--kafka-bootstrap", default=_env("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default=_env("TOPIC", "news_events_raw_v1"))
    parser.add_argument("--source-system", default=_env("SOURCE_SYSTEM", "ingest_news_api"))
    parser.add_argument("--limit", type=int, default=int(_env("LIMIT", "20")))
    parser.add_argument("--seed", type=int, default=int(_env("SEED", "42")))
    parser.add_argument("--timeout-seconds", type=int, default=int(_env("TIMEOUT_SECONDS", "10")))
    parser.add_argument("--dry-run", action="store_true", help="Fetch and print envelopes, do not publish.")
    args = parser.parse_args(argv)

    request_id = str(uuid.uuid4())
    run_id = _env("RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"))

    api_url = args.api_base_url.rstrip("/") + "/v1/raw/news"
    resp = requests.get(
        api_url,
        params={"limit": args.limit, "seed": args.seed},
        timeout=args.timeout_seconds,
    )
    resp.raise_for_status()
    payload = resp.json()
    items = payload.get("items", [])
    if not isinstance(items, list):
        raise ValueError("mock api response must contain list field 'items'")

    fetched_at = _utc_iso_now()

    envelopes: list[dict[str, Any]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        envelopes.append(
            _build_envelope(
                raw_item=item,
                source_system=args.source_system,
                fetched_at=fetched_at,
                ingested_at=_utc_iso_now(),
                run_id=run_id,
                request_id=request_id,
            )
        )

    if args.dry_run:
        for e in envelopes[:3]:
            sys.stdout.write(json.dumps(e, ensure_ascii=False) + "\n")
        sys.stdout.write(f"dry_run=true envelopes={len(envelopes)}\n")
        return 0

    producer = KafkaProducer(
        bootstrap_servers=_normalize_bootstrap(args.kafka_bootstrap),
        key_serializer=lambda b: b,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=50,
        retries=3,
    )

    sent = 0
    for envelope in envelopes:
        producer.send(args.topic, key=_message_key(envelope), value=envelope)
        sent += 1

    producer.flush(timeout=args.timeout_seconds)
    producer.close(timeout=args.timeout_seconds)
    sys.stdout.write(
        f"published topic={args.topic} messages={sent} seed={args.seed} limit={args.limit} run_id={run_id}\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

