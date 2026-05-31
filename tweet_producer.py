import time
import random
from kafka import KafkaProducer

# --- Expanded, balanced tweet pool ---
tweets = [
    # Positive (30)
    "I absolutely love this product! Best purchase ever.",
    "Wow, amazing experience today at the concert!",
    "Best day of my life, everything went perfectly!",
    "So happy with my new phone, highly recommend!",
    "Incredible sunset today, feeling grateful!",
    "Just got promoted at work, so excited!",
    "This coffee shop is absolutely wonderful, 10/10.",
    "My team won the championship today, unbelievable!",
    "Just finished an amazing book, highly recommend it.",
    "The new update is fantastic, everything works perfectly.",
    "Feeling so blessed and grateful for today.",
    "Our wedding anniversary dinner was absolutely perfect.",
    "Just adopted a puppy, best decision of my life!",
    "The customer service here is outstanding, truly impressed.",
    "Finally finished my thesis! So relieved and happy.",
    "This new restaurant is incredible, best food in town.",
    "Got my dream job offer today, couldn't be happier!",
    "The concert was phenomenal, they were amazing live.",
    "Spring weather finally here, feeling great outside.",
    "My flight was smooth and on time, great airline!",
    "Surprised my mom with flowers today, she was overjoyed.",
    "Just ran my first 5K! So proud of myself.",
    "The new park in our neighborhood is beautiful.",
    "Loving the new season of this show, absolutely brilliant.",
    "Great teamwork today, we crushed the project deadline.",
    "Yoga class this morning was so refreshing and peaceful.",
    "Can't stop smiling, today was just perfect.",
    "The delivery arrived early and everything was perfect.",
    "My kids' school play was absolutely adorable.",
    "Just booked my dream vacation, so excited!",
    # Negative (30)
    "This is terrible. Completely disappointed.",
    "I hate waiting in long lines, so frustrating.",
    "This movie was absolutely boring and a waste of time.",
    "Terrible traffic again, this city is a nightmare.",
    "Worst service ever, never coming back.",
    "My package arrived broken, completely unacceptable.",
    "This app keeps crashing, I'm so done with it.",
    "Terrible customer support, waited 2 hours for nothing.",
    "The food was cold and tasteless, huge disappointment.",
    "I lost my wallet today, worst day ever.",
    "Flight cancelled with no notice, absolutely furious.",
    "This product broke after one week, total waste of money.",
    "Horrible experience at the clinic, staff was rude.",
    "My internet has been down all day, so annoying.",
    "Raining again on my day off, what a letdown.",
    "Got a parking ticket for no reason, so unfair.",
    "The new policy at work is awful, everyone hates it.",
    "Waited 45 minutes for food that tasted terrible.",
    "My laptop died right before the deadline, I'm panicking.",
    "Noise complaints from neighbors again, unbearable.",
    "The gym was filthy today, totally disgusting.",
    "My subscription was charged twice, this is outrageous.",
    "Road construction has made my commute a nightmare.",
    "Terrible hotel experience, never booking here again.",
    "The update broke everything, I want the old version back.",
    "So disappointed with the game result, awful performance.",
    "My order was wrong again for the third time.",
    "This traffic jam has cost me an important meeting.",
    "The movie theater was loud and filthy, never returning.",
    "Spent hours on hold just to get disconnected.",
    # Neutral (20)
    "Just had lunch, nothing special.",
    "The weather is okay I guess.",
    "Feeling neutral about the whole situation.",
    "Just finished a meeting, pretty normal day.",
    "Had coffee this morning, it was fine.",
    "Took the bus to work today instead of driving.",
    "Reading a book this evening, fairly average story.",
    "Watched TV for a bit, nothing interesting on.",
    "Grocery shopping done, got the usual stuff.",
    "Working from home again today.",
    "The meeting ran a little long but wrapped up.",
    "Ordered takeout, it was average.",
    "Checked my emails, nothing urgent.",
    "Walked to the store, weather was mild.",
    "Finished the report and submitted it.",
    "Went to bed early last night.",
    "Looked at the news, things are happening I suppose.",
    "Updated my phone software today.",
    "Scheduled a dentist appointment for next week.",
    "Packed lunch today, nothing exciting.",
]

# --- Connect to Kafka ---
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: v.encode('utf-8')
)

print("🚀 Producer started — sending tweets to Kafka topic 'tweets'...")
print(f"   Pool: {sum(1 for t in tweets if 'love' in t.lower() or 'amazing' in t.lower() or 'happy' in t.lower())} positive-ish | "
      f"Total pool: {len(tweets)} tweets")

try:
    while True:
        tweet = random.choice(tweets)
        producer.send('tweets', value=tweet)
        print(f"  ➤ Sent: {tweet[:70]}{'...' if len(tweet) > 70 else ''}")
        time.sleep(0.5)  # 2 tweets per second

except KeyboardInterrupt:
    print("\n⛔ Producer stopped.")
    producer.close()