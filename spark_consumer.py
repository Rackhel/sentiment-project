"""
spark_consumer.py — Enhanced for Final Presentation
Reads JSON from Kafka, classifies sentiment, outputs:
  - compound_score  (raw VADER float, e.g. 0.743)
  - sentiment       (positive / negative / neutral)
  - topic           (tech / sports / weather / food / work)
to CSV, console, and an alert engine.
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, window, col, current_timestamp, from_json
from pyspark.sql.types import StringType, FloatType, StructType, StructField

# ── Logging setup ────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log"),
    ]
)
log = logging.getLogger("SentimentStream")

# ── VADER setup ───────────────────────────────────────────────────────────────
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _analyzer = SentimentIntensityAnalyzer()
    log.info("VADER analyzer loaded successfully.")
except ImportError as e:
    log.error(f"Failed to import vaderSentiment: {e}")
    raise

# ── UDFs ──────────────────────────────────────────────────────────────────────
def get_compound(text):
    """Return raw VADER compound score (float -1.0 to +1.0)."""
    try:
        if not text or text.strip() == "":
            return 0.0
        return float(_analyzer.polarity_scores(text)['compound'])
    except Exception as e:
        log.warning(f"Compound score error for '{str(text)[:50]}': {e}")
        return 0.0

def classify_sentiment(text):
    """Classify tweet sentiment using VADER compound score."""
    try:
        if not text or text.strip() == "":
            return "neutral"
        score = _analyzer.polarity_scores(text)['compound']
        if score >= 0.05:
            return "positive"
        elif score <= -0.05:
            return "negative"
        else:
            return "neutral"
    except Exception as e:
        log.warning(f"Sentiment classification error for '{str(text)[:50]}': {e}")
        return "neutral"

compound_udf  = udf(get_compound,        FloatType())
sentiment_udf = udf(classify_sentiment,  StringType())

# ── JSON schema ───────────────────────────────────────────────────────────────
# Producer now sends: {"tweet": "...", "topic": "tech", "source": "simulated"}
json_schema = StructType([
    StructField("tweet",  StringType(), True),
    StructField("topic",  StringType(), True),
    StructField("source", StringType(), True),
])

# ── Spark Session ─────────────────────────────────────────────────────────────
try:
    spark = SparkSession.builder \
        .appName("SentimentStream") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    log.info("Spark session started successfully.")
except Exception as e:
    log.critical(f"Failed to start Spark session: {e}")
    raise

# ── Read from Kafka ───────────────────────────────────────────────────────────
try:
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "tweets") \
        .option("startingOffsets", "latest") \
        .load()
    log.info("Kafka stream connected (topic: tweets, port: 9092).")
except Exception as e:
    log.critical(f"Failed to connect to Kafka: {e}")
    raise

# ── Parse JSON + enrich ───────────────────────────────────────────────────────
parsed = raw_stream \
    .selectExpr("CAST(value AS STRING) as raw_json") \
    .select(from_json(col("raw_json"), json_schema).alias("data")) \
    .select(
        col("data.tweet").alias("tweet"),
        col("data.topic").alias("topic"),
        col("data.source").alias("source"),
    )

tweets_df = parsed \
    .withColumn("timestamp",      current_timestamp()) \
    .withColumn("compound_score", compound_udf(col("tweet"))) \
    .withColumn("sentiment",      sentiment_udf(col("tweet")))

# ── Windowed counts (by sentiment) ───────────────────────────────────────────
windowed_sentiment = tweets_df \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("sentiment")
    ).count() \
    .orderBy("window", "sentiment")

# ── Windowed counts (by topic) ────────────────────────────────────────────────
windowed_topic = tweets_df \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("topic")
    ).count() \
    .orderBy("window", "topic")

# ── Alert engine ──────────────────────────────────────────────────────────────
ALERT_THRESHOLD = 10

def alert_on_negative_spike(batch_df, batch_id):
    """Fire an alert when a 10-sec window has ≥ ALERT_THRESHOLD negative tweets."""
    try:
        rows = batch_df.collect()
        for row in rows:
            if row["sentiment"] == "negative" and row["count"] >= ALERT_THRESHOLD:
                ws = row["window"]["start"].strftime("%H:%M:%S")
                we = row["window"]["end"].strftime("%H:%M:%S")
                msg = (
                    f"🚨 ALERT [{ws}–{we}] "
                    f"Negative spike: {row['count']} tweets (threshold: {ALERT_THRESHOLD})"
                )
                log.warning(msg)
                print(f"\n{msg}\n")
    except Exception as e:
        log.error(f"Alert check failed for batch {batch_id}: {e}")

# ── Output: console (sentiment counts) ───────────────────────────────────────
console_query = windowed_sentiment.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# ── Output: alert engine ──────────────────────────────────────────────────────
alert_query = windowed_sentiment.writeStream \
    .outputMode("complete") \
    .foreachBatch(alert_on_negative_spike) \
    .trigger(processingTime="10 seconds") \
    .start()

# ── Output: CSV (tweet-level, with compound_score + topic) ───────────────────
csv_query = tweets_df \
    .select("tweet", "topic", "source", "timestamp", "compound_score", "sentiment") \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/sentiment_results") \
    .option("checkpointLocation", "output/checkpoint") \
    .option("header", True) \
    .trigger(processingTime="10 seconds") \
    .start()

log.info("📡 All stream queries started. Listening for tweets... (Ctrl+C to stop)")
print("\n📡 Listening for tweets... (Ctrl+C to stop)")
print(f"   Columns written: tweet | topic | source | timestamp | compound_score | sentiment")
print(f"   🚨 Alert threshold: {ALERT_THRESHOLD} negative tweets / 10-sec window\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    log.info("Pipeline stopped by user.")
    print("\n⛔ Pipeline stopped.")