"""
NewsPulse — Producer API GNews
Polling GNews API setiap 10 menit, kirim ke Kafka topic: news-api
"""

import json
import time
import hashlib
import requests
from datetime import datetime
from kafka import KafkaProducer

# ── Konfigurasi ──────────────────────────────────────
GNEWS_API_KEY  = "8799cfeaf912adae1279f4cea4595ee5"   # daftar di gnews.io
GNEWS_ENDPOINT = "https://gnews.io/api/v4/top-headlines"
KAFKA_BROKER   = "localhost:9092"
KAFKA_TOPIC    = "news-api"
POLL_INTERVAL  = 1200

# NewsAPI.org sebagai alternatif kalau GNews habis quota
NEWSAPI_KEY      = "GANTI_DENGAN_NEWSAPI_KEY"  # daftar di newsapi.org
NEWSAPI_ENDPOINT = "https://newsapi.org/v2/top-headlines"

# ── Setup Kafka Producer ──────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    enable_idempotence=True,
    acks="all",
    retries=5
)

# ── Track artikel yang sudah dikirim (hindari duplikat) ──
sent_urls = set()

def fetch_gnews():
    """Ambil berita dari GNews API."""
    params = {
        "country": "id",
        "lang":    "id",
        "max":     10,
        "token":   GNEWS_API_KEY
    }
    try:
        resp = requests.get(GNEWS_ENDPOINT, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("articles", [])
    except Exception as e:
        print(f"[ERROR] GNews API gagal: {e}")
        return []

def fetch_newsapi():
    """Alternatif: ambil berita dari NewsAPI.org."""
    params = {
        "country":  "id",
        "pageSize": 10,
        "apiKey":   NEWSAPI_KEY
    }
    try:
        resp = requests.get(NEWSAPI_ENDPOINT, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        articles = data.get("articles", [])
        # Normalisasi ke format yang sama dengan GNews
        normalized = []
        for a in articles:
            normalized.append({
                "title":       a.get("title", ""),
                "description": a.get("description", ""),
                "url":         a.get("url", ""),
                "publishedAt": a.get("publishedAt", ""),
                "source":      {"name": a.get("source", {}).get("name", "NewsAPI")},
                "category":    "general"
            })
        return normalized
    except Exception as e:
        print(f"[ERROR] NewsAPI gagal: {e}")
        return []

def parse_article(article, source_api="gnews"):
    """Normalisasi artikel ke format JSON yang konsisten."""
    url        = article.get("url", "")
    title      = article.get("title", "")
    desc       = article.get("description", "") or ""
    published  = article.get("publishedAt", "") or ""
    source     = article.get("source", {}).get("name", "Unknown")
    category   = article.get("category", "general")

    # Buat ID unik dari URL
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]

    return {
        "id":          url_hash,
        "title":       title,
        "description": desc[:300],   # potong supaya tidak terlalu panjang
        "url":         url,
        "source":      source,
        "category":    category,
        "published_at": published,
        "fetched_at":  datetime.now().isoformat(),
        "source_api":  source_api
    }

def send_to_kafka(article_data):
    """Kirim satu artikel ke Kafka topic news-api."""
    key      = article_data.get("category", "general")
    url      = article_data.get("url", "")

    # Skip kalau sudah pernah dikirim
    if url in sent_urls:
        return False

    try:
        producer.send(
            KAFKA_TOPIC,
            key=key,
            value=article_data
        )
        sent_urls.add(url)
        return True
    except Exception as e:
        print(f"[ERROR] Gagal kirim ke Kafka: {e}")
        return False

def main():
    print("=" * 55)
    print("  NewsPulse — Producer API (GNews)")
    print("  Kafka Topic: news-api")
    print(f"  Polling interval: {POLL_INTERVAL // 60} menit")
    print("=" * 55)

    cycle = 0
    while True:
        cycle += 1
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Cycle #{cycle} — Fetching GNews API...")

        # Coba GNews dulu, fallback ke NewsAPI
        articles = fetch_gnews()
        source_api = "gnews"

        if not articles:
            print("  GNews kosong, mencoba NewsAPI...")
            articles = fetch_newsapi()
            source_api = "newsapi"

        if not articles:
            print("  Semua API gagal, skip cycle ini.")
        else:
            sent = 0
            for article in articles:
                data = parse_article(article, source_api)
                if data["title"] and data["url"]:
                    if send_to_kafka(data):
                        sent += 1
                        print(f"  ✅ [{data['source']}] {data['title'][:60]}...")

            producer.flush()
            print(f"  → {sent} artikel baru dikirim ke Kafka ({len(articles) - sent} duplikat dilewati)")

        print(f"  Menunggu {POLL_INTERVAL // 60} menit...")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
