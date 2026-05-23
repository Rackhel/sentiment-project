import time
import random
from kafka import KafkaProducer

# --- Sample tweets pool ---
tweets = [
    "I absolutely love this product! Best purchase ever.",
    "This is terrible. Completely disappointed.",
    "Just had lunch, nothing special.",
    "Wow, amazing experience today at the concert!",
    "I hate waiting in long lines, so frustrating.",
    "The weather is okay I guess.",
    "Best day of my life, everything went perfectly!",
    "Worst service ever, never coming back.",
    "Feeling neutral about the whole situation.",
    "So happy with my new phone, highly recommend!",
    "This movie was absolutely boring and a waste of time.",
    "Just finished a meeting, pretty normal day.",
    "Incredible sunset today, feeling grateful!",
    "Terrible traffic again, this city is a nightmare.",
    "Had coffee this morning, it was fine.",
]

# --- Connect to Kafka ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8')
)

print("🚀 Producer started — sending tweets to Kafka topic 'tweets'...")

try:
    while True:
        tweet = random.choice(tweets)
        producer.send('tweets', value=tweet)
        print(f"  ➤ Sent: {tweet}")
        time.sleep(0.5)  # 2 tweets per second

except KeyboardInterrupt:
    print("\n⛔ Producer stopped.")
    producer.close()