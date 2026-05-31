from pyspark.sql import SparkSession
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from kafka import KafkaProducer

print("✅ PySpark:", SparkSession.builder.appName("test").getOrCreate().version)
print("✅ VADER:", SentimentIntensityAnalyzer().polarity_scores("I love big data!"))
print("✅ kafka-python: imported OK")