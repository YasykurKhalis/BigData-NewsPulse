# 🌍 NewsPulse: Analisis Tren Berita Nasional

**ETS Big Data & Data Lakehouse — Kelompok 7**

---

## 👥 Anggota Kelompok

| Nama | NRP | Tugas |
|---|---|---|
| Yasykur Khalis Jati Maulana Yuwono | 5027241112 | Setup Docker (Hadoop + Kafka), Infrastructure |
| Ryan Adya Purwanto | 5027231046 | `producer_api.py` + `spark/analysis.ipynb` |
| Hanif Mawla Faizi | 5027241064 | `producer_rss.py` + `consumer_to_hdfs.py` + `dashboard/app.py` |

---

## 📌 Topik & Skenario

**NewsPulse** adalah sistem monitoring tren berita nasional yang dibangun untuk klien PR agency.

> **Pertanyaan Bisnis:**
> *"Topik apa yang paling hangat hari ini di berbagai media, dan jam berapa biasanya berita dominan muncul?"*

---

## 🏗️ Arsitektur Sistem

```
[GNews API]        [Kompas RSS + Tempo RSS]
     │                       │
     ▼                       ▼
┌─────────────┐     ┌─────────────────┐
│producer_    │     │producer_rss.py  │
│api.py       │     │(2 RSS sekaligus)│
└──────┬──────┘     └────────┬────────┘
       │                     │
       ▼                     ▼
╔══════════════════════════════════╗
║         APACHE KAFKA             ║
║  Topic: news-api  Topic: news-rss║
╚══════════════╤═══════════════════╝
               │
               ▼
       ┌───────────────┐
       │consumer_to_   │
       │hdfs.py        │
       └───────┬───────┘
               │
               ▼
╔══════════════════════════════════╗
║           HADOOP HDFS            ║
║  /data/news/api/                 ║
║  /data/news/rss/                 ║
║  /data/news/hasil/               ║
╚══════════════╤═══════════════════╝
               │
               ▼
       ┌───────────────┐
       │Apache Spark   │
       │analysis.ipynb │
       └───────┬───────┘
               │
               ▼
       ┌───────────────┐
       │Flask Dashboard│
       │localhost:5000 │
       └───────────────┘
```

---

## 🚀 Cara Menjalankan

### Prasyarat
```bash
pip install kafka-python feedparser requests hdfs flask
```

### Langkah 1 — Jalankan Hadoop
```bash
docker compose -f docker-compose-hadoop.yml up -d
# Tunggu ~60 detik
docker compose -f docker-compose-hadoop.yml ps
```

Buat direktori di HDFS:
```bash
docker exec -it hadoop-namenode bash
hdfs dfs -mkdir -p /data/news/api
hdfs dfs -mkdir -p /data/news/rss
hdfs dfs -mkdir -p /data/news/hasil
exit
```

### Langkah 2 — Jalankan Kafka
```bash
docker compose -f docker-compose-kafka.yml up -d
# Tunggu ~30 detik
```

Buat topic:
```bash
docker exec -it kafka-broker kafka-topics.sh \
  --create --topic news-api \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

docker exec -it kafka-broker kafka-topics.sh \
  --create --topic news-rss \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

Verifikasi topic:
```bash
docker exec -it kafka-broker kafka-topics.sh \
  --list --bootstrap-server localhost:9092
```

### Langkah 3 — Jalankan Producer & Consumer
Buka **3 terminal terpisah**:

```bash
# Terminal 1 - Producer API (GNews)
python kafka/producer_api.py

# Terminal 2 - Producer RSS (Kompas + Tempo)
python kafka/producer_rss.py

# Terminal 3 - Consumer ke HDFS
python kafka/consumer_to_hdfs.py
```

### Langkah 4 — Jalankan Analisis Spark
Buka `spark/analysis.ipynb` di Google Colab atau Jupyter, jalankan semua cell.

### Langkah 5 — Jalankan Dashboard
```bash
python dashboard/app.py
```
Buka browser: **http://localhost:5000**

---

## ✅ Verifikasi Sistem

```bash
# Cek event masuk Kafka
docker exec -it kafka-broker kafka-console-consumer.sh \
  --topic news-api --from-beginning \
  --bootstrap-server localhost:9092

# Cek file di HDFS
docker exec -it hadoop-namenode bash
hdfs dfs -ls -R /data/news/
```

---

## 📸 Screenshot

*(akan diisi saat demo)*

- [ ] HDFS Web UI (localhost:9870)
- [ ] Kafka consumer output
- [ ] Dashboard berjalan (localhost:5000)

---

## 🧩 Tantangan & Solusi

*(akan diisi setelah pengerjaan selesai)*

---

## 📦 Teknologi yang Digunakan

| Teknologi | Versi | Fungsi |
|---|---|---|
| Apache Kafka | 3.x | Message broker / ingestion layer |
| Apache Hadoop | 3.3.6 | Distributed storage (HDFS) |
| Apache Spark | 3.x | Batch processing & analisis |
| Flask | 3.x | Web dashboard |
| Python | 3.x | Semua script |
| Docker | - | Containerisasi Hadoop & Kafka |
