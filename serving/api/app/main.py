from __future__ import annotations

import random
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import FastAPI, Query


app = FastAPI(title="ai_ops mock upstream API", version="0.1.0")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


@dataclass(frozen=True)
class _Template:
    provider: str
    channel: str
    title: str
    body: str


_TEMPLATES: list[_Template] = [
    _Template(
        provider="mockwire",
        channel="rss",
        title="BREAKING: {company} shares spike {pct}% after rumor",
        body="<p>{company} is said to be exploring <b>strategic options</b>... source: {anon}</p>",
    ),
    _Template(
        provider="fastnews",
        channel="webhook",
        title="{company} 发布公告：{topic}",
        body="【快讯】{company}：{topic}。详见：{url}  \n\n（转发）",
    ),
    _Template(
        provider="socialpulse",
        channel="scrape",
        title="{company} trending — {topic} 👀",
        body="hot take: {topic}... no link, just vibes. $ {ticker}   ",
    ),
]


_COMPANIES = [
    ("Apple", "AAPL"),
    ("Tesla", "TSLA"),
    ("NVIDIA", "NVDA"),
    ("贵州茅台", "600519.SH"),
    ("宁德时代", "300750.SZ"),
]

_TOPICS = [
    "earnings beat",
    "profit warning",
    "regulatory update",
    "new product launch",
    "CEO interview",
    "宏观数据不及预期",
    "行业政策利好",
]

_ANON_SOURCES = ["someone familiar", "a trader", "a blog", "微博用户", "匿名渠道"]


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _dirty_published_at(rng: random.Random, base: datetime) -> Any:
    roll = rng.random()
    if roll < 0.15:
        return None
    if roll < 0.35:
        return int(base.timestamp())
    if roll < 0.55:
        return base.strftime("%Y/%m/%d %H:%M:%S")
    if roll < 0.75:
        return base.astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%dT%H:%M:%S+08:00")
    return base.strftime("%a, %d %b %Y %H:%M:%S GMT")


def _maybe_invalid_url(rng: random.Random, company: str) -> str | None:
    roll = rng.random()
    if roll < 0.12:
        return None
    if roll < 0.20:
        return "htp://bad-url"
    slug = re.sub(r"[^a-z0-9]+", "-", company.lower()).strip("-") or "item"
    return f"https://example.com/news/{slug}"


def _generate_one(rng: random.Random, idx: int) -> dict[str, Any]:
    company, ticker = rng.choice(_COMPANIES)
    topic = rng.choice(_TOPICS)
    anon = rng.choice(_ANON_SOURCES)
    pct = rng.randint(1, 15)

    template = rng.choice(_TEMPLATES)
    url = _maybe_invalid_url(rng, company)

    now = datetime.now(timezone.utc)
    base = now - timedelta(seconds=rng.randint(0, 3600))

    title = template.title.format(company=company, topic=topic, pct=pct)
    body = template.body.format(company=company, topic=topic, anon=anon, ticker=ticker, url=url or "")

    # Intentionally messy: whitespace, duplicated symbols, mixed types.
    symbols: Any = [ticker, ticker, company] if rng.random() < 0.35 else [ticker]
    importance: Any = str(rng.randint(1, 5)) if rng.random() < 0.4 else rng.randint(1, 5)

    provider_message_id = str(uuid.uuid4()) if rng.random() < 0.8 else None

    item: dict[str, Any] = {
        "provider_message_id": provider_message_id,
        "provider": template.provider,
        "channel": template.channel,
        "title": title + ("!!!" if rng.random() < 0.2 else ""),
        "body": body,
        "url": url,
        "published_at": _dirty_published_at(rng, base),
        "language": rng.choice(["en", "zh", "und", None]),
        "symbols": symbols,
        "importance": importance,
        "raw": {
            "ingest_seq": idx,
            "dup_hint": rng.choice([None, "repost", "mirror", ""]),
        },
    }

    # Occasionally omit fields to simulate bad providers.
    if rng.random() < 0.12:
        item.pop("body", None)
    if rng.random() < 0.08:
        item.pop("title", None)
    return item


@app.get("/v1/raw/news")
def raw_news(
    limit: int = Query(default=10, ge=1, le=200),
    seed: int = Query(default=1, ge=1, le=1_000_000),
) -> dict[str, Any]:
    rng = random.Random(seed)
    items = [_generate_one(rng, i) for i in range(limit)]

    # Add one deterministic near-duplicate to make downstream dedup easy to demonstrate.
    if limit >= 5:
        items[1]["url"] = items[0].get("url")
        items[1]["provider_message_id"] = items[0].get("provider_message_id")
        items[1]["title"] = (items[0].get("title") or "") + " (repost)"

    return {"generated_at": _iso(datetime.now(timezone.utc)), "items": items}

