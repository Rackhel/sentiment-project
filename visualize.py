"""
visualize.py — Enhanced for Final Presentation
Generates a 2-row, 4-chart summary PNG from output/sentiment_results/*.csv

Charts:
  Row 1: Overall sentiment bar | Sentiment over time | Sentiment pie
  Row 2: Topic breakdown bar   | Compound score histogram | Sentiment × Topic heatmap
"""

import sys
import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import matplotlib.colors as mcolors

ALERT_THRESHOLD = 10  # must match spark_consumer.py

# ── Load data ─────────────────────────────────────────────────────────────────
csv_files = glob.glob("output/sentiment_results/*.csv")
if not csv_files:
    print("❌ No CSV files found. Run spark_consumer.py first.")
    sys.exit(1)

try:
    df = pd.concat([pd.read_csv(f) for f in csv_files], ignore_index=True)
except Exception as e:
    print(f"❌ Failed to load CSV files: {e}")
    sys.exit(1)

df = df.dropna(subset=["sentiment", "timestamp"])
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
df = df.dropna(subset=["timestamp"])

if "compound_score" in df.columns:
    df["compound_score"] = pd.to_numeric(df["compound_score"], errors="coerce")
if "topic" not in df.columns:
    df["topic"] = "unknown"
df["topic"] = df["topic"].fillna("unknown")

if df.empty:
    print("❌ No valid records after cleaning.")
    sys.exit(1)

print(f"✅ Loaded {len(df)} records from {len(csv_files)} CSV file(s)")
print(df["sentiment"].value_counts().to_string())

# ── Colors ────────────────────────────────────────────────────────────────────
sent_colors  = {"positive": "#2ecc71", "negative": "#e74c3c", "neutral": "#95a5a6"}
topic_colors = {"tech": "#3498db", "sports": "#e67e22", "weather": "#1abc9c",
                "food": "#9b59b6", "work": "#f39c12", "unknown": "#bdc3c7"}

# ── Figure layout ─────────────────────────────────────────────────────────────
fig, axes = plt.subplots(2, 3, figsize=(20, 10))
fig.suptitle(
    "Real-Time Social Media Sentiment Analytics — Final Results Summary",
    fontsize=15, fontweight="bold", y=1.01
)

# ── [0,0] Overall sentiment bar ───────────────────────────────────────────────
ax = axes[0, 0]
counts = df["sentiment"].value_counts()
bars = ax.bar(counts.index, counts.values,
              color=[sent_colors.get(s, "#bdc3c7") for s in counts.index],
              edgecolor="white", linewidth=1.5, width=0.5)
ax.set_title("Overall Sentiment Distribution", fontweight="bold")
ax.set_xlabel("Sentiment"); ax.set_ylabel("Tweet Count")
ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
for bar, val in zip(bars, counts.values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
            str(val), ha="center", va="bottom", fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)

# ── [0,1] Sentiment over time ─────────────────────────────────────────────────
ax = axes[0, 1]
df["window"] = df["timestamp"].dt.floor("10s")
time_series = df.groupby(["window", "sentiment"]).size().unstack(fill_value=0)
for sentiment in ["positive", "negative", "neutral"]:
    if sentiment in time_series.columns:
        ax.plot(time_series.index, time_series[sentiment],
                label=sentiment.capitalize(),
                color=sent_colors[sentiment], marker="o", markersize=4, linewidth=2)
if "negative" in time_series.columns:
    for ts, val in time_series["negative"].items():
        if val >= ALERT_THRESHOLD:
            ax.annotate(f"🚨 {val}", xy=(ts, val), xytext=(0, 8),
                        textcoords="offset points", ha="center",
                        fontsize=8, color=sent_colors["negative"])
ax.set_title("Sentiment Counts Over Time (10s windows)", fontweight="bold")
ax.set_xlabel("Time Window"); ax.set_ylabel("Tweet Count")
ax.legend(); ax.tick_params(axis="x", rotation=30)
ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
ax.spines[["top", "right"]].set_visible(False)

# ── [0,2] Sentiment pie ───────────────────────────────────────────────────────
ax = axes[0, 2]
pie_counts = df["sentiment"].value_counts()
ax.pie(pie_counts.values,
       labels=[s.capitalize() for s in pie_counts.index],
       colors=[sent_colors.get(s, "#bdc3c7") for s in pie_counts.index],
       autopct="%1.1f%%", startangle=140,
       wedgeprops={"edgecolor": "white", "linewidth": 2})
ax.set_title("Sentiment Share (%)", fontweight="bold")

# ── [1,0] Topic breakdown ─────────────────────────────────────────────────────
ax = axes[1, 0]
topic_counts = df["topic"].value_counts()
tbars = ax.bar(topic_counts.index, topic_counts.values,
               color=[topic_colors.get(t, "#bdc3c7") for t in topic_counts.index],
               edgecolor="white", linewidth=1.5, width=0.5)
ax.set_title("Tweet Volume by Topic", fontweight="bold")
ax.set_xlabel("Topic"); ax.set_ylabel("Tweet Count")
ax.yaxis.set_major_locator(ticker.MaxNLocator(integer=True))
for bar, val in zip(tbars, topic_counts.values):
    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.3,
            str(val), ha="center", va="bottom", fontweight="bold")
ax.spines[["top", "right"]].set_visible(False)

# ── [1,1] Compound score histogram ───────────────────────────────────────────
ax = axes[1, 1]
if "compound_score" in df.columns and df["compound_score"].notna().any():
    for sentiment in ["positive", "negative", "neutral"]:
        subset = df[df["sentiment"] == sentiment]["compound_score"].dropna()
        if not subset.empty:
            ax.hist(subset, bins=25, alpha=0.65, color=sent_colors[sentiment],
                    label=sentiment.capitalize(), edgecolor="white")
    ax.axvline(0.05,  color=sent_colors["positive"], linestyle="--", linewidth=1.2,
               label="Pos threshold (0.05)")
    ax.axvline(-0.05, color=sent_colors["negative"], linestyle="--", linewidth=1.2,
               label="Neg threshold (−0.05)")
    ax.set_title("VADER Compound Score Distribution", fontweight="bold")
    ax.set_xlabel("Compound Score (−1 to +1)"); ax.set_ylabel("Frequency")
    ax.legend(fontsize=8)
    ax.spines[["top", "right"]].set_visible(False)
else:
    ax.text(0.5, 0.5, "compound_score\nnot available",
            ha="center", va="center", transform=ax.transAxes, fontsize=12, color="gray")
    ax.set_title("VADER Compound Score Distribution", fontweight="bold")

# ── [1,2] Sentiment × Topic heatmap ──────────────────────────────────────────
ax = axes[1, 2]
known_topics = [t for t in ["tech", "sports", "weather", "food", "work"]
                if t in df["topic"].values]
if known_topics:
    pivot = df[df["topic"].isin(known_topics)] \
        .groupby(["topic", "sentiment"]).size().unstack(fill_value=0)
    for s in ["positive", "negative", "neutral"]:
        if s not in pivot.columns:
            pivot[s] = 0
    pivot = pivot[["positive", "negative", "neutral"]]
    pivot_pct = pivot.div(pivot.sum(axis=1), axis=0).mul(100)

    im = ax.imshow(pivot_pct.values, cmap="RdYlGn", aspect="auto", vmin=0, vmax=100)
    ax.set_xticks(range(3))
    ax.set_xticklabels(["Positive %", "Negative %", "Neutral %"])
    ax.set_yticks(range(len(pivot_pct.index)))
    ax.set_yticklabels([t.capitalize() for t in pivot_pct.index])
    for i in range(pivot_pct.shape[0]):
        for j in range(pivot_pct.shape[1]):
            ax.text(j, i, f"{pivot_pct.values[i, j]:.1f}%",
                    ha="center", va="center", fontweight="bold", fontsize=10)
    plt.colorbar(im, ax=ax, fraction=0.046, pad=0.04, label="% of topic tweets")
    ax.set_title("Sentiment × Topic Heatmap (%)", fontweight="bold")
else:
    ax.text(0.5, 0.5, "Topic data\nnot available",
            ha="center", va="center", transform=ax.transAxes, fontsize=12, color="gray")
    ax.set_title("Sentiment × Topic Heatmap", fontweight="bold")

plt.tight_layout()

try:
    plt.savefig("output/sentiment_chart.png", dpi=150, bbox_inches="tight")
    print("\n✅ Chart saved to output/sentiment_chart.png")
except Exception as e:
    print(f"❌ Failed to save chart: {e}")
    sys.exit(1)

plt.show()