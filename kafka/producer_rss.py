"""
NewsPulse — Producer RSS (Kompas + Tempo)
Polling 2 RSS feed setiap 5 menit, kirim ke Kafka topic: news-rss
"""

import json
import time
import hashlib
import feedparser
from datetime import datetime
from kafka import KafkaProducer

# ── Konfigurasi ──────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"
KAFKA_TOPIC   = "news-rss"
POLL_INTERVAL = 300

RSS_FEEDS = [
    {
        "name":   "Kompas Nasional",
        "url":    "https://rss.kompas.com/feed/kompas.com/nasional",
        "source": "Kompas"
    },
    {
        "name":   "Tempo Nasional",
        "url":    "https://rss.tempo.co/nasional",
        "source": "Tempo"
    }
]

# ── Setup Kafka Producer ──────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    enable_idempotence=True,
    acks="all",
    retries=5
)

# ── Track artikel yang sudah dikirim ─────────────────
sent_urls = set()

def make_hash(url):
    """Buat hash 8 karakter dari URL."""
    return hashlib.md5(url.encode()).hexdigest()[:8]

def parse_entry(entry, source_name):
    """Normalisasi satu entry RSS ke format JSON konsisten."""
    url     = entry.get("link", "")
    title   = entry.get("title", "")
    summary = entry.get("summary", "") or ""

    # Parsing waktu publikasi
    published_parsed = entry.get("published_parsed")
    if published_parsed:
        published_at = datetime(*published_parsed[:6]).isoformat()
    else:
        published_at = datetime.now().isoformat()

    return {
        "id":          make_hash(url),
        "title":       title,
        "description": summary[:300],
        "url":         url,
        "source":      source_name,
        "published_at": published_at,
        "fetched_at":  datetime.now().isoformat(),
        "source_api":  "rss"
    }

def fetch_rss(feed_config):
    """Fetch dan parse satu RSS feed."""
    try:
        feed = feedparser.parse(feed_config["url"])
        if feed.bozo:
            print(f"  [WARNING] {feed_config['name']}: feed parsing warning")
        return feed.entries
    except Exception as e:
        print(f"  [ERROR] {feed_config['name']} gagal: {e}")
        return []

def send_to_kafka(article_data):
    """Kirim satu artikel ke Kafka topic news-rss."""
    url = article_data.get("url", "")

    if url in sent_urls:
        return False

    try:
        key = make_hash(url)
        producer.send(
            KAFKA_TOPIC,
            key=key,
            value=article_data
        )
        sent_urls.add(url)
        return True
    except Exception as e:
        print(f"  [ERROR] Gagal kirim ke Kafka: {e}")
        return False

def main():
    print("=" * 55)
    print("  NewsPulse — Producer RSS")
    print("  Kafka Topic: news-rss")
    print(f"  RSS Feeds: {', '.join(f['name'] for f in RSS_FEEDS)}")
    print(f"  Polling interval: {POLL_INTERVAL // 60} menit")
    print("=" * 55)

    cycle = 0
    while True:
        cycle += 1
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Cycle #{cycle} — Fetching RSS feeds...")

        total_sent = 0

        for feed_config in RSS_FEEDS:
            print(f"\n  📰 {feed_config['name']}...")
            entries = fetch_rss(feed_config)

            sent = 0
            for entry in entries:
                data = parse_entry(entry, feed_config["source"])
                if data["title"] and data["url"]:
                    if send_to_kafka(data):
                        sent += 1
                        print(f"    ✅ {data['title'][:60]}...")

            print(f"    → {sent} artikel baru dari {feed_config['name']}")
            total_sent += sent

        producer.flush()
        print(f"\n  Total: {total_sent} artikel baru dikirim ke Kafka")
        print(f"  Menunggu {POLL_INTERVAL // 60} menit...")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
