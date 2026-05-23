from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, window, col, current_timestamp
from pyspark.sql.types import StringType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# --- Sentiment classifier ---
analyzer = SentimentIntensityAnalyzer()

def classify_sentiment(text):
    if text is None:
        return "neutral"
    score = analyzer.polarity_scores(text)['compound']
    if score >= 0.05:
        return "positive"
    elif score <= -0.05:
        return "negative"
    else:
        return "neutral"

sentiment_udf = udf(classify_sentiment, StringType())

# --- Spark Session ---
spark = SparkSession.builder \
    .appName("SentimentStream") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR") # suppress INFO noise

print("✅ Spark session started, reading from Kafka...")

# --- Read from Kafka ---
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets") \
    .option("startingOffsets", "latest") \
    .load()

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

# --- Output to console ---
console_query = windowed.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

# --- Output to CSV ---
csv_query = tweets_df \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "output/sentiment_results") \
    .option("checkpointLocation", "output/checkpoint") \
    .option("header", True) \
    .trigger(processingTime="10 seconds") \
    .start()

print("📡 Listening for tweets... (Ctrl+C to stop)\n")
spark.streams.awaitAnyTermination()
