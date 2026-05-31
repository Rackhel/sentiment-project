import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, window, col, current_timestamp
from pyspark.sql.types import StringType

# --- Logging setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("pipeline.log"),
    ]
)
log = logging.getLogger("SentimentStream")

# --- Sentiment classifier with error handling ---
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    _analyzer = SentimentIntensityAnalyzer()
    log.info("VADER analyzer loaded successfully.")
except ImportError as e:
    log.error(f"Failed to import vaderSentiment: {e}")
    raise

def classify_sentiment(text):
    """Classify tweet sentiment using VADER compound score."""
    try:
        if text is None or text.strip() == "":
            return "neutral"
        score = _analyzer.polarity_scores(text)['compound']
        if score >= 0.05:
            return "positive"
        elif score <= -0.05:
            return "negative"
        else:
            return "neutral"
    except Exception as e:
        log.warning(f"Sentiment classification error for text '{text[:50]}': {e}")
        return "neutral"

sentiment_udf = udf(classify_sentiment, StringType())

# --- Spark Session ---
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

# --- Read from Kafka ---
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

# --- Parse and classify ---
tweets_df = raw_stream.selectExpr("CAST(value AS STRING) as tweet") \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("sentiment", sentiment_udf(col("tweet")))

# --- Windowed sentiment counts (10-second tumbling window) ---
windowed = tweets_df \
    .groupBy(
        window(col("timestamp"), "10 seconds"),
        col("sentiment")
    ).count() \
    .orderBy("window", "sentiment")

# --- Alerting: track negative counts per window via foreachBatch ---
ALERT_THRESHOLD = 10  # alert if negative tweets >= this in a 10-sec window

def alert_on_negative_spike(batch_df, batch_id):
    """Check each micro-batch for negative sentiment spikes and alert."""
    try:
        rows = batch_df.collect()
        for row in rows:
            sentiment = row["sentiment"]
            count = row["count"]
            window_start = row["window"]["start"].strftime("%H:%M:%S")
            window_end   = row["window"]["end"].strftime("%H:%M:%S")
            if sentiment == "negative" and count >= ALERT_THRESHOLD:
                log.warning(
                    f"🚨 ALERT [{window_start}–{window_end}] "
                    f"Negative spike detected: {count} negative tweets "
                    f"(threshold: {ALERT_THRESHOLD})"
                )
                print(
                    f"\n🚨 NEGATIVE SPIKE ALERT [{window_start}–{window_end}]: "
                    f"{count} negative tweets in this window!\n"
                )
    except Exception as e:
        log.error(f"Alert check failed for batch {batch_id}: {e}")

# --- Output to console ---
console_query = windowed.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# --- Alert query (foreachBatch on windowed counts) ---
alert_query = windowed.writeStream \
    .outputMode("complete") \
    .foreachBatch(alert_on_negative_spike) \
    .trigger(processingTime="10 seconds") \
    .start()

# --- Output to CSV (raw tweet level) ---
csv_query = tweets_df \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/sentiment_results") \
    .option("checkpointLocation", "output/checkpoint") \
    .option("header", True) \
    .trigger(processingTime="10 seconds") \
    .start()

log.info("📡 All stream queries started. Listening for tweets... (Ctrl+C to stop)")
print("\n📡 Listening for tweets... (Ctrl+C to stop)\n")
print(f"   🚨 Negative spike alert threshold: {ALERT_THRESHOLD} tweets / 10-sec window\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    log.info("Pipeline stopped by user.")
    print("\n⛔ Pipeline stopped.")