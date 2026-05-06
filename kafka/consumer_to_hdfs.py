"""
NewsPulse — Consumer Kafka → HDFS
Baca dari news-api dan news-rss, simpan ke HDFS setiap 2 menit
Juga simpan salinan lokal untuk dashboard
"""

import json
import os
import subprocess
import threading
import time
from datetime import datetime
from kafka import KafkaConsumer

# ── Konfigurasi ──────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
TOPICS         = ["news-api", "news-rss"]
GROUP_ID       = "newspulse-consumer"
FLUSH_INTERVAL = 120   # flush ke HDFS setiap 2 menit
HDFS_BASE      = "/data/news"
LOCAL_TEMP_DIR = "./temp_buffer"
DASHBOARD_DIR  = "./dashboard/data"

# ── Buffer untuk kedua topic ──────────────────────────
buffers = {
    "news-api": [],
    "news-rss": []
}
buffer_lock = threading.Lock()

# ── Setup directory lokal ─────────────────────────────
os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
os.makedirs(DASHBOARD_DIR, exist_ok=True)

def save_to_hdfs(data_list, hdfs_path, local_temp_path):
    """Simpan data ke HDFS via subprocess (Opsi A — mudah)."""
    if not data_list:
        return False

    # Simpan ke file lokal dulu
    with open(local_temp_path, "w", encoding="utf-8") as f:
        json.dump(data_list, f, ensure_ascii=False, indent=2)

    # Push ke HDFS
    try:
        result = subprocess.run(
            ["docker", "exec", "hadoop-namenode",
             "hdfs", "dfs", "-put", "-f",
             f"/host{local_temp_path}",
             hdfs_path],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0:
            return True
        else:
            # Fallback: copy file ke container dulu
            subprocess.run(
                ["docker", "cp", local_temp_path,
                 f"hadoop-namenode:/tmp/{os.path.basename(local_temp_path)}"],
                timeout=30
            )
            subprocess.run(
                ["docker", "exec", "hadoop-namenode",
                 "hdfs", "dfs", "-put", "-f",
                 f"/tmp/{os.path.basename(local_temp_path)}",
                 hdfs_path],
                timeout=30
            )
            return True
    except Exception as e:
        print(f"  [ERROR] HDFS upload gagal: {e}")
        return False

def update_dashboard_files():
    """Update file JSON lokal untuk dashboard."""
    with buffer_lock:
        api_data = buffers["news-api"][-20:] if buffers["news-api"] else []
        rss_data = buffers["news-rss"][-20:] if buffers["news-rss"] else []

    # Gabungkan untuk live feed
    all_news = sorted(
        api_data + rss_data,
        key=lambda x: x.get("fetched_at", ""),
        reverse=True
    )[:30]

    with open(f"{DASHBOARD_DIR}/live_api.json", "w", encoding="utf-8") as f:
        json.dump(api_data, f, ensure_ascii=False, indent=2)

    with open(f"{DASHBOARD_DIR}/live_rss.json", "w", encoding="utf-8") as f:
        json.dump(rss_data, f, ensure_ascii=False, indent=2)

    with open(f"{DASHBOARD_DIR}/live_all.json", "w", encoding="utf-8") as f:
        json.dump(all_news, f, ensure_ascii=False, indent=2)

def flush_to_hdfs():
    """Flush buffer ke HDFS secara periodik."""
    while True:
        time.sleep(FLUSH_INTERVAL)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")

        with buffer_lock:
            for topic in TOPICS:
                data = buffers[topic].copy()
                buffers[topic] = []   # reset buffer

                if not data:
                    continue

                # Tentukan path HDFS
                subfolder  = "api" if topic == "news-api" else "rss"
                hdfs_path  = f"{HDFS_BASE}/{subfolder}/{timestamp}.json"
                local_path = f"{LOCAL_TEMP_DIR}/{subfolder}_{timestamp}.json"

                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Flushing {len(data)} events dari {topic} ke HDFS...")
                success = save_to_hdfs(data, hdfs_path, local_path)

                if success:
                    print(f"  ✅ Tersimpan: hdfs://{hdfs_path}")
                else:
                    print(f"  ❌ Gagal simpan ke HDFS")

        update_dashboard_files()

def consume_topic(topic):
    """Consumer untuk satu topic Kafka."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms=1000
    )

    print(f"  ✅ Consumer siap untuk topic: {topic}")

    while True:
        try:
            for message in consumer:
                data = message.value
                with buffer_lock:
                    buffers[topic].append(data)
                print(f"  [{topic}] ← {data.get('source', '?')} | {data.get('title', '')[:50]}...")
        except Exception as e:
            if "timeout" not in str(e).lower():
                print(f"  [ERROR] Consumer {topic}: {e}")
        time.sleep(0.1)

def main():
    print("=" * 55)
    print("  NewsPulse — Consumer → HDFS")
    print(f"  Topics: {', '.join(TOPICS)}")
    print(f"  Flush interval: {FLUSH_INTERVAL // 60} menit")
    print(f"  HDFS Base: {HDFS_BASE}")
    print("=" * 55)

    # Thread flush ke HDFS
    flush_thread = threading.Thread(target=flush_to_hdfs, daemon=True)
    flush_thread.start()

    # Thread consumer per topic
    threads = []
    for topic in TOPICS:
        t = threading.Thread(target=consume_topic, args=(topic,), daemon=True)
        t.start()
        threads.append(t)

    print(f"\n✅ Consumer berjalan — monitoring {len(TOPICS)} topic...")
    print("  Tekan Ctrl+C untuk berhenti\n")

    try:
        while True:
            time.sleep(10)
            with buffer_lock:
                api_count = len(buffers["news-api"])
                rss_count = len(buffers["news-rss"])
            print(f"  [Buffer] news-api: {api_count} | news-rss: {rss_count} events")
    except KeyboardInterrupt:
        print("\n\nConsumer dihentikan.")

if __name__ == "__main__":
    main()
