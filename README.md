# Real-Time Social Media Sentiment Analytics System

**Students:** Rackhel Fernando L.B. & Sheikh MD Sifat
**Student IDs:** 202312229 | 202312254
**Course:** Big Data Platform — Spring 2026

---

## Project Overview

A real-time streaming pipeline that simulates social media tweets across five topics,
classifies their sentiment using VADER, outputs windowed counts every 10 seconds,
fires alerts on negative spikes, and displays a live Streamlit dashboard —
built on Apache Kafka and Spark Structured Streaming.

---

## Architecture

```text
tweet_producer.py
JSON {tweet, topic, source}
│
▼
Apache Kafka (topic: tweets)
│
▼
Spark Structured Streaming
│  from_json → VADER UDF → compound_score + sentiment
│
├──▶ Console (windowed counts every 10s)
├──▶ Alert engine (🚨 if negative ≥ 10 in a window)
├──▶ pipeline.log (structured log file)
└──▶ CSV output (tweet | topic | source | timestamp | compound_score | sentiment)
         │
         ▼
    dashboard.py  ←  Streamlit live dashboard (auto-refreshes every 5s)
```

---

## Tools & Versions

| Tool | Version |
|---|---|
| Python | 3.11 |
| PySpark | 3.5.0 |
| Apache Kafka | confluentinc/cp-kafka:7.4.0 |
| Zookeeper | confluentinc/cp-zookeeper:7.4.0 |
| VADER Sentiment | 3.3.2 |
| Streamlit | ≥ 1.33 |
| Plotly | ≥ 5.0 |
| Java | 17 (openjdk) |
| OS | Ubuntu (WSL2 on Windows) |

---

## Setup Instructions

### 1. Prerequisites
- WSL2 with Ubuntu
- Docker Desktop (WSL2 backend enabled)
- VS Code with WSL extension

### 2. Install Java 17
```bash
sudo apt update && sudo apt install -y openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

### 3. Create Python 3.11 virtual environment
```bash
python3.11 -m venv venv
source venv/bin/activate
pip install pyspark==3.5.0 kafka-python vaderSentiment streamlit plotly pandas matplotlib
```

### 4. Start Kafka with Docker
```bash
docker compose up -d
```

### 5. Run the pipeline
Open **three** terminals:

**Terminal 1 — Producer:**
```bash
source venv/bin/activate
python tweet_producer.py
```

**Terminal 2 — Consumer (Spark):**
```bash
source venv/bin/activate
python spark_consumer.py
```

**Terminal 3 — Live Dashboard:**
```bash
source venv/bin/activate
streamlit run dashboard.py
```
Open http://localhost:8501 in your browser.

---

## Features

### Topic Classification
The producer tags each tweet with one of five topics: **tech, sports, weather, food, work**.
Messages are sent as JSON `{"tweet": "...", "topic": "tech", "source": "simulated"}`.
Spark parses the JSON using `from_json()` and passes the topic field through to the CSV output.
The dashboard and `visualize.py` both display a topic breakdown chart and a
**Sentiment × Topic heatmap** showing the negative/positive rate per topic.

### Compound Score Output
`spark_consumer.py` now writes a `compound_score` column (raw VADER float, −1.0 to +1.0)
to every CSV row alongside the sentiment label. This allows post-hoc analysis of
score distributions and is visualised as a histogram in both the dashboard and `visualize.py`.

### Negative Spike Alerting
`spark_consumer.py` checks every 10-second window via `foreachBatch`. If **10 or more**
negative tweets appear in a single window, it prints a `🚨 ALERT` to the console and
logs it to `pipeline.log`. Threshold is configurable via `ALERT_THRESHOLD` at the top
of the file.

### Error Handling & Logging
- All components wrapped in `try/except` with descriptive log messages.
- `pipeline.log` written alongside the script for post-run inspection.
- Malformed or empty tweets return `"neutral"` / score `0.0` with a warning log.
- Bad CSV files in the dashboard are skipped gracefully.
- Old CSVs without a `topic` or `compound_score` column are handled via fallback defaults.

### Balanced Tweet Pool
The producer draws from **80 tweets** (30 positive / 30 negative / 20 neutral),
evenly spread across five topics, producing a realistic multi-dimensional dataset.

### Live Streamlit Dashboard
`dashboard.py` auto-refreshes every 5 seconds and shows:
- KPI metrics: total, positive %, negative %, neutral %, avg compound score
- 🚨 Alert banner when a negative spike is detected
- Bar chart + donut chart (overall sentiment distribution)
- **Topic volume bar chart** (tweet counts per topic)
- **Compound score histogram** (overlapping by sentiment)
- **Sentiment × Topic heatmap** (% positive/negative/neutral per topic)
- Time-series line chart (per-window counts)
- Scrollable recent tweets table with score + topic columns

---

## Errors Encountered & Solutions

### Error 1: Wrong Java version
**Problem:** PySpark 3.5.0 requires Java 11 or 17. JAVA_HOME was unset.
**Solution:** Installed Java 17, set JAVA_HOME permanently in ~/.bashrc.

### Error 2: Python 3.14 serialization error
**Problem:** UDF failed with `RecursionError` / `PicklingError`. PySpark is not yet
compatible with Python 3.14.
**Solution:** Created a new venv using Python 3.11.

### Error 3: Streamlit duplicate auto-generated element ID
**Problem:** Dashboard crashed on second refresh: `StreamlitDuplicateElementId`.
**Solution:** Added unique `key=` arguments to all `st.plotly_chart()` calls.

### Error 4: Streamlit duplicate element key (while loop)
**Problem:** Static `key=` inside a `while True` loop caused `StreamlitDuplicateElementKey`.
**Solution:** Removed the `while True` loop; replaced with `time.sleep(5)` + `st.rerun()`.

### Warning: KAFKA-1894
**Message:** `KafkaDataConsumer is not running in UninterruptibleThread`
**Solution:** Known harmless warning — log level set to ERROR to suppress.

---

## Output

### Console output (every 10 seconds)
```text
+------------------------------------------+---------+-----+
|window                                    |sentiment|count|
+------------------------------------------+---------+-----+
|{2026-06-08 14:22:00, 2026-06-08 14:22:10}|negative |8    |
|{2026-06-08 14:22:00, 2026-06-08 14:22:10}|positive |12   |
+------------------------------------------+---------+-----+

🚨 NEGATIVE SPIKE ALERT [14:22:00–14:22:10]: 11 negative tweets in this window!
```

### CSV output schema
```
tweet, topic, source, timestamp, compound_score, sentiment
```
Raw tweet-level records saved to `output/sentiment_results/`.

### Live Dashboard
Streamlit dashboard at http://localhost:8501 — auto-refreshes every 5s.

### Visualization
Static 6-chart summary from `visualize.py` → `output/sentiment_chart.png`.

### Log file
Structured log at `pipeline.log` — INFO/WARNING/ERROR entries for every
pipeline event, including all spike alerts.

---

## File Structure

```text
sentiment-project/
├── venv/                    # Python virtual environment
├── docker-compose.yml       # Kafka + Zookeeper containers
├── tweet_producer.py        # JSON producer — 80-tweet pool, 5 topics
├── spark_consumer.py        # Spark stream → VADER → compound_score + topic → CSV + alerts
├── dashboard.py             # Streamlit live dashboard (6 charts, auto-refresh 5s)
├── visualize.py             # Static 6-chart PNG export
├── pipeline.log             # Structured log (generated at runtime)
└── output/
    ├── sentiment_results/   # CSV output (tweet|topic|timestamp|compound_score|sentiment)
    ├── checkpoint/          # Spark streaming checkpoint
    └── sentiment_chart.png  # Static chart from visualize.py
```