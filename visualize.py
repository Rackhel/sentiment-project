import glob
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from collections import Counter

# --- Load all CSV files from output folder ---
csv_files = glob.glob("output/sentiment_results/*.csv")

if not csv_files:
    print("❌ No CSV files found. Run spark_consumer.py first to generate output.")
    exit()

df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
df = df.dropna(subset=["sentiment", "timestamp"])
df["timestamp"] = pd.to_datetime(df["timestamp"])

print(f"✅ Loaded {len(df)} records from {len(csv_files)} CSV files")
print(df["sentiment"].value_counts())

# --- Colors ---
colors = {
    "positive": "#2ecc71",
    "negative": "#e74c3c",
    "neutral":  "#95a5a6"
}

fig, axes = plt.subplots(1, 3, figsize=(16, 5))
fig.suptitle("Real-Time Social Media Sentiment Analysis\nResults Summary",
             fontsize=14, fontweight="bold", y=1.02)

# --- Chart 1: Overall sentiment bar chart ---
ax1 = axes[0]
counts = df["sentiment"].value_counts()
bars = ax1.bar(counts.index,
               counts.values,
               color=[colors.get(s, "#bdc3c7") for s in counts.index],
               edgecolor="white", linewidth=1.5, width=0.5)
ax1.set_title("Overall Sentiment Distribution", fontweight="bold")
ax1.set_xlabel("Sentiment")
ax1.set_ylabel("Tweet Count")
ax1.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
for bar, val in zip(bars, counts.values):
    ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
             str(val), ha="center", va="bottom", fontweight="bold")
ax1.spines[["top", "right"]].set_visible(False)

# --- Chart 2: Sentiment over time (line chart) ---
ax2 = axes[1]
df["window"] = df["timestamp"].dt.floor("10s")
time_series = df.groupby(["window", "sentiment"]).size().unstack(fill_value=0)
for sentiment in ["positive", "negative", "neutral"]:
    if sentiment in time_series.columns:
        ax2.plot(time_series.index, time_series[sentiment],
                 label=sentiment.capitalize(),
                 color=colors[sentiment],
                 marker="o", markersize=4, linewidth=2)
ax2.set_title("Sentiment Counts Over Time", fontweight="bold")
ax2.set_xlabel("Time Window")
ax2.set_ylabel("Tweet Count")
ax2.legend()
ax2.tick_params(axis="x", rotation=30)
ax2.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
ax2.spines[["top", "right"]].set_visible(False)

# --- Chart 3: Pie chart ---
ax3 = axes[2]
pie_counts = df["sentiment"].value_counts()
ax3.pie(pie_counts.values,
        labels=[s.capitalize() for s in pie_counts.index],
        colors=[colors.get(s, "#bdc3c7") for s in pie_counts.index],
        autopct="%1.1f%%",
        startangle=140,
        wedgeprops={"edgecolor": "white", "linewidth": 2})
ax3.set_title("Sentiment Share (%)", fontweight="bold")

plt.tight_layout()
plt.savefig("output/sentiment_chart.png", dpi=150, bbox_inches="tight")
print("\n✅ Chart saved to output/sentiment_chart.png")
plt.show()