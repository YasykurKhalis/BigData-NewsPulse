# 🌍 NewsPulse — Monitor Tren Berita Nasional

**ETS Praktik Big Data dan Data Lakehouse — Kelompok 7**

> "Topik apa yang paling hangat hari ini di berbagai media, dan jam berapa biasanya berita dominan muncul?"

---

## 👥 Anggota Kelompok & Kontribusi

| Nama | NRP | Kontribusi |
|------|-----|------------|
| Ryan Adya Purwanto | 5027231046 | Setup Docker (Hadoop & Kafka), Spark analysis.ipynb (3 analisis wajib), integrasi pipeline end-to-end |
| Yasykur Khalis | 5027241112 | producer_api.py (integrasi GNews API), producer_rss.py (RSS Kompas & Tempo) |
| Hanif Mawla Faizi | 5027241064 | consumer_to_hdfs.py (consumer Kafka → HDFS), dashboard/app.py + index.html (Flask dashboard) |

---

## 📌 Topik yang Dipilih

**Topik 5 — NewsPulse: Analisis Tren Berita Nasional**

Sistem monitoring berita nasional real-time untuk PR agency yang perlu memantau isu paling banyak dibicarakan media Indonesia hari ini. Data diambil dari GNews API dan RSS feed Kompas & Tempo setiap beberapa menit, diproses dengan Apache Spark, dan ditampilkan di dashboard Flask.

---

## 🏗️ Arsitektur Sistem

```
[GNews API]          [RSS Kompas & Tempo]
     │                       │
     ▼                       ▼
┌──────────────┐     ┌──────────────┐
│producer_api  │     │producer_rss  │
│    .py       │     │    .py       │
└──────┬───────┘     └──────┬───────┘
       │                    │
       ▼                    ▼
╔══════════════════════════════════╗
║         APACHE KAFKA             ║
║  Topic: news-api  Topic: news-rss║
╚══════════════╤═══════════════════╝
               │
               ▼
       ┌───────────────┐
       │consumer_to_   │
       │  hdfs.py      │
       └──────┬────────┘
              │
              ▼
╔══════════════════════════════════╗
║           HADOOP HDFS            ║
║  /data/news/api/                 ║
║  /data/news/rss/                 ║
╚══════════════╤═══════════════════╝
               │
               ▼
       ┌───────────────┐
       │ Apache Spark  │
       │analysis.ipynb │
       └──────┬────────┘
              │
              ▼
       ┌───────────────┐
       │  Dashboard    │
       │ Flask :5000   │
       └───────────────┘
```

---

## 🚀 Cara Menjalankan Sistem

### Prasyarat
- Docker Desktop (WSL2 backend)
- Python 3.11
- Java 17
- Apache Spark 4.1.1

### Step 1 — Clone Repository

```bash
git clone https://github.com/YasykurKhalis/BigData-NewsPulse.git
cd BigData-NewsPulse
python -m venv venv311
venv311\Scripts\activate
pip install kafka-python feedparser flask requests
```

### Step 2 — Jalankan Hadoop (HDFS)

```bash
docker-compose -f docker-compose-hadoop.yml up -d
```

Tunggu ~20 detik, lalu buat direktori HDFS:

```bash
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/news/rss
docker exec hadoop-namenode hdfs dfs -mkdir -p /data/news/api
```

Verifikasi:
```bash
docker exec hadoop-namenode hdfs dfsadmin -report
```
Pastikan muncul `Live datanodes (1)`.

### Step 3 — Jalankan Kafka

```bash
docker-compose -f docker-compose-kafka.yml up -d
```

Verifikasi topic:
```bash
docker exec kafka-broker /usr/bin/kafka-topics --list --bootstrap-server localhost:9092
```

### Step 4 — Jalankan Producer & Consumer

Buka **4 terminal terpisah**, semua dari root folder:

```bash
# Terminal 1 — Producer API
cd kafka && python producer_api.py
```

```bash
# Terminal 2 — Producer RSS
cd kafka && python producer_rss.py
```

```bash
# Terminal 3 — Consumer ke HDFS
cd kafka && python consumer_to_hdfs.py
```

```bash
# Terminal 4 — Dashboard
python dashboard\app.py
```

### Step 5 — Jalankan Spark Analysis

Buka `spark/analysis.ipynb` di VS Code, jalankan semua cell dari atas ke bawah.

> **Catatan:** Sebelum menjalankan Spark, pastikan file JSON dari HDFS sudah dicopy ke folder `spark/` (lihat bagian Catatan Keterbatasan).

### Step 6 — Buka Dashboard

Buka browser: `http://localhost:5000`

---

## 📊 Hasil Analisis Spark

### Analisis 1 — Kata Paling Sering di Judul Berita
Menggunakan `split()` + `explode()` + filter stopwords di Spark SQL.

**Hasil:** Kata paling sering muncul adalah **sekolah** (7x) dan **2026** (7x), diikuti **prabowo** (6x) — mencerminkan isu pendidikan dan politik dominan di media nasional hari ini.

### Analisis 2 — Distribusi Berita per Sumber
Menggunakan `groupBy source` + window function untuk persentase.

**Hasil:** Tempo mendominasi dengan 50 artikel (80.6%), diikuti detiksport (4 artikel), CNN Indonesia (2 artikel), dan 6 sumber lainnya.

### Analisis 3 — Volume Publikasi per Jam
Menggunakan `HOUR(TO_TIMESTAMP(published_at))` + `groupBy` di Spark SQL.

**Hasil:** Jam tersibuk adalah **08:00 WIB** dengan 14 artikel — media nasional paling aktif mempublikasikan berita di pagi hari.

---

## 🖼️ Screenshot

### HDFS Web UI (localhost:9870)
> Screenshot HDFS menunjukkan file JSON tersimpan di `/data/news/rss/` dan `/data/news/api/`

### Kafka Consumer Output
> Screenshot terminal consumer menunjukkan artikel masuk dari topic `news-rss` dan `news-api`

### Dashboard (localhost:5000)
> Screenshot dashboard menampilkan 3 panel Spark (kata trending, distribusi sumber, volume per jam) dan panel berita live dari Kafka

---

## ⚠️ Catatan Keterbatasan

Pada implementasi ini, Apache Spark **membaca file JSON dari lokal** (bukan langsung dari HDFS) karena keterbatasan network Docker di Windows Home (WSL2 backend). Spark yang berjalan di Windows host tidak dapat menjangkau IP internal Docker container (`172.18.0.x`) untuk mengakses Datanode secara langsung.

**Workaround yang dilakukan:**
1. Data tetap dikirim ke HDFS melalui pipeline Kafka → Consumer → HDFS (terbukti dengan `hdfs dfs -ls`)
2. File JSON dicopy dari HDFS ke lokal menggunakan `docker cp`
3. Spark membaca dari file lokal tersebut

**Referensi:** Sesuai FAQ soal ETS — *"Spark boleh membaca file lokal sebagai alternatif, tapi catat keterbatasan ini di README."*

---

## 🔍 Verifikasi Pipeline

```bash
# Cek HDFS
docker exec hadoop-namenode hdfs dfs -ls -R /data/news/

# Cek Kafka topic
docker exec kafka-broker /usr/bin/kafka-topics --list --bootstrap-server localhost:9092

# Cek consumer group
docker exec kafka-broker /usr/bin/kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group newspulse-consumer
```

---

## 💡 Tantangan & Solusi

| Tantangan | Solusi |
|-----------|--------|
| Spark tidak bisa connect ke HDFS Datanode dari Windows | Copy file dari HDFS ke lokal via `docker cp`, baca dari lokal |
| BlockMissingException saat Datanode restart | Down Hadoop dengan `-v` flag untuk bersihkan volume lama |
| IP Docker berubah setiap restart | Tambah static hostname di `/etc/hosts` Windows |
| Consumer menulis ke path yang salah | Fix `DASHBOARD_DIR` dari `./dashboard/data` ke `../dashboard/data` |

---

## 📁 Struktur Repository

```
BigData-NewsPulse/
├── README.md
├── docker-compose-hadoop.yml
├── docker-compose-kafka.yml
├── hadoop.env
├── kafka/
│   ├── producer_api.py       # Yasykur — GNews API producer
│   ├── producer_rss.py       # Yasykur — RSS Kompas & Tempo producer
│   └── consumer_to_hdfs.py   # Hanif — Consumer Kafka → HDFS
├── spark/
│   └── analysis.ipynb        # Ryan — 3 Spark analysis wajib
└── dashboard/
    ├── app.py                # Hanif — Flask web app
    ├── templates/
    │   └── index.html        # Hanif — Dashboard UI
    ├── static/
    └── data/                 # (di .gitignore)
        ├── spark_results.json
        ├── live_api.json
        └── live_rss.json
```
