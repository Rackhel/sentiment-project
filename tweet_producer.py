"""
tweet_producer.py — Enhanced for Final Presentation
Sends JSON messages to Kafka with 'tweet', 'topic', and 'source' fields.
Topics: tech, sports, weather, food, work
"""

import time
import json
import random
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Tweet pool — each entry is (text, topic)
# Distribution: 30 positive / 30 negative / 20 neutral  (same balance as before)
# Topics: tech, sports, weather, food, work
# ---------------------------------------------------------------------------
tweets = [
    # ── POSITIVE ────────────────────────────────────────────────────────────
    ("I absolutely love this new smartphone, best purchase ever!",           "tech"),
    ("The new software update is fantastic, everything works perfectly.",    "tech"),
    ("Just built my first PC — it runs like a dream!",                      "tech"),
    ("This AI assistant is incredible, it saves me hours every day.",       "tech"),
    ("Finally found a framework that makes coding genuinely fun.",          "tech"),
    ("Our team won the championship today, unbelievable celebration!",      "sports"),
    ("Just ran my first 5K — so proud of myself!",                         "sports"),
    ("The match was absolutely thrilling, best game of the season.",        "sports"),
    ("My favourite team crushed it tonight, incredible performance!",       "sports"),
    ("Great workout session today, feeling unstoppable.",                   "sports"),
    ("Incredible sunset today, the sky was absolutely stunning.",           "weather"),
    ("Spring weather is finally here — perfect day to be outside!",        "weather"),
    ("Beautiful morning for a walk, sunny and warm.",                      "weather"),
    ("Love this crisp autumn air, such a refreshing change.",              "weather"),
    ("Perfect beach weather today, couldn't ask for more!",                "weather"),
    ("This new restaurant is incredible — best food in the city.",         "food"),
    ("Just had the most amazing brunch, highly recommend it.",             "food"),
    ("My homemade sourdough turned out absolutely perfect today.",         "food"),
    ("This coffee shop is wonderful — 10/10, will return.",               "food"),
    ("The delivery arrived hot and on time — delicious!",                  "food"),
    ("Just got promoted at work, so excited about the new role!",          "work"),
    ("Great teamwork today — we crushed the project deadline.",            "work"),
    ("My manager gave amazing feedback on the presentation.",              "work"),
    ("Finally finished my thesis — so relieved and happy!",               "work"),
    ("Got my dream job offer today, couldn't be happier!",                "work"),
    ("Wow, amazing live concert experience today!",                        "sports"),
    ("Just adopted a puppy — best decision of my life!",                  "food"),    # lifestyle → food bucket
    ("Our wedding anniversary dinner was absolutely perfect.",             "food"),
    ("Surprised my mum with flowers today — she was overjoyed.",          "work"),
    ("Just booked my dream vacation, so excited!",                        "weather"),

    # ── NEGATIVE ────────────────────────────────────────────────────────────
    ("This app keeps crashing — I'm completely done with it.",             "tech"),
    ("The update broke everything, I want the old version back.",          "tech"),
    ("My laptop died right before the deadline, totally panicking.",       "tech"),
    ("Terrible WiFi again — how is this still a problem in 2026?",        "tech"),
    ("Lost all my data because the cloud sync failed. Unacceptable.",     "tech"),
    ("Worst referee decision ever — completely ruined the match.",         "sports"),
    ("So disappointed with the team's performance, awful game.",          "sports"),
    ("Injury took out our best player, season might be over.",            "sports"),
    ("Lost the final in the last second — heartbreaking.",               "sports"),
    ("The gym was filthy and overcrowded today, totally disgusting.",     "sports"),
    ("Terrible traffic again — this commute is an absolute nightmare.",   "weather"),
    ("Raining again on my day off, what a letdown.",                     "weather"),
    ("The heatwave is unbearable, AC broke at the worst time.",          "weather"),
    ("Flooded roads made me an hour late — so stressful.",               "weather"),
    ("Storm knocked out power all night — worst sleep ever.",            "weather"),
    ("The food was cold and tasteless, huge disappointment.",             "food"),
    ("Waited 45 minutes for food that tasted terrible.",                  "food"),
    ("My order was wrong again for the third time this week.",           "food"),
    ("Found hair in my meal — completely unacceptable.",                  "food"),
    ("This restaurant charged me twice and the service was rude.",        "food"),
    ("Terrible customer support — waited 2 hours for nothing.",          "work"),
    ("The new policy at work is awful, everyone hates it.",              "work"),
    ("Spent hours on hold just to get disconnected.",                    "work"),
    ("My subscription was charged twice — this is outrageous.",          "work"),
    ("Road construction has cost me yet another important meeting.",      "work"),
    ("This product broke after one week — total waste of money.",        "tech"),
    ("Flight cancelled with no notice — absolutely furious.",            "weather"),
    ("Got a parking ticket for no reason — so unfair.",                  "work"),
    ("Noise complaints from neighbours again — completely unbearable.",   "work"),
    ("This traffic jam has been going for two hours straight.",          "weather"),

    # ── NEUTRAL ─────────────────────────────────────────────────────────────
    ("Just had lunch, nothing special today.",                            "food"),
    ("Ordered takeout — it was average, nothing exciting.",              "food"),
    ("Grocery shopping done, got the usual stuff.",                      "food"),
    ("Watched the match, it ended in a draw.",                          "sports"),
    ("The weather is okay I suppose, a bit cloudy.",                    "weather"),
    ("Took the bus to work today instead of driving.",                  "work"),
    ("Finished the report and submitted it on time.",                   "work"),
    ("Checked my emails — nothing urgent in the inbox.",                "work"),
    ("Updated my phone software today.",                               "tech"),
    ("Scheduled a dentist appointment for next week.",                  "work"),
    ("Working from home again, same as usual.",                        "work"),
    ("The meeting ran a little long but wrapped up fine.",             "work"),
    ("Looked at the news — things are happening I suppose.",           "work"),
    ("Reading a book this evening, fairly average story.",             "work"),
    ("Walked to the store, weather was mild.",                        "weather"),
    ("Went to bed early last night.",                                  "work"),
    ("Had coffee this morning — it was fine.",                        "food"),
    ("Packed lunch today, nothing exciting.",                          "food"),
    ("Just ran a system update, everything seems normal.",            "tech"),
    ("Attended the seminar — covered the basics.",                    "work"),
]

# ---------------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_counts = {}
for _, topic in tweets:
    topic_counts[topic] = topic_counts.get(topic, 0) + 1

print("🚀 Producer started — sending JSON tweets to Kafka topic 'tweets'...")
print(f"   Pool: {len(tweets)} tweets across topics: {dict(sorted(topic_counts.items()))}")
print("   Format: JSON {tweet, topic, source}\n")

try:
    while True:
        text, topic = random.choice(tweets)
        payload = {
            "tweet":  text,
            "topic":  topic,
            "source": "simulated",
        }
        producer.send('tweets', value=payload)
        print(f"  ➤ [{topic:7s}] {text[:65]}{'...' if len(text) > 65 else ''}")
        time.sleep(0.5)   # 2 messages / second

except KeyboardInterrupt:
    print("\n⛔ Producer stopped.")
    producer.close()