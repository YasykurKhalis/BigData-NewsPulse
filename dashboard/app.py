"""
dashboard/app.py
Hanif Mawla Faizi (5027241064)
NewsPulse — Flask Dashboard
Menampilkan hasil Spark + data live dari Kafka
"""

import json
import os
from flask import Flask, render_template, jsonify
from datetime import datetime

app = Flask(__name__)

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

def load_json(filename, default=None):
    """Load file JSON dengan fallback jika tidak ada."""
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return default if default is not None else {}

@app.route("/")
def index():
    """Halaman utama dashboard."""
    return render_template("index.html")

@app.route("/api/data")
def api_data():
    """Endpoint JSON untuk semua data — di-fetch oleh frontend."""
    spark_results = load_json("spark_results.json", {})
    live_api      = load_json("live_api.json", [])
    live_rss      = load_json("live_rss.json", [])
    live_all      = load_json("live_all.json", [])

    # Gabungkan & urutkan live news
    all_news = sorted(
        live_api + live_rss,
        key=lambda x: x.get("fetched_at", ""),
        reverse=True
    )[:20]

    return jsonify({
        "spark":       spark_results,
        "live_api":    live_api[:10],
        "live_rss":    live_rss[:10],
        "live_all":    all_news,
        "last_update": datetime.now().strftime("%H:%M:%S"),
        "total_api":   len(live_api),
        "total_rss":   len(live_rss)
    })

@app.route("/api/spark")
def api_spark():
    """Endpoint khusus untuk hasil Spark."""
    return jsonify(load_json("spark_results.json", {}))

@app.route("/api/live")
def api_live():
    """Endpoint khusus untuk berita live terbaru."""
    live_all = load_json("live_all.json", [])
    return jsonify(live_all[:20])

if __name__ == "__main__":
    print("=" * 50)
    print("  NewsPulse Dashboard")
    print("  Buka: http://localhost:5000")
    print("=" * 50)
    app.run(debug=True, host="0.0.0.0", port=5000)
