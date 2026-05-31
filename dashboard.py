"""
Real-Time Sentiment Dashboard — Streamlit
Reads from output/sentiment_results/*.csv and auto-refreshes every 5 seconds.
Run with: streamlit run dashboard.py
"""

import glob
import time
import pandas as pd
import streamlit as st
import plotly.express as px
from datetime import datetime

# --- Page config ---
st.set_page_config(
    page_title="Sentiment Analytics Dashboard",
    page_icon="📊",
    layout="wide",
)

REFRESH_INTERVAL = 5  # seconds
ALERT_THRESHOLD  = 10

COLORS = {
    "positive": "#2ecc71",
    "negative": "#e74c3c",
    "neutral":  "#95a5a6",
}

# --- Helper: load data ---
def load_data():
    csv_files = glob.glob("output/sentiment_results/*.csv")
    if not csv_files:
        return None, 0
    frames = []
    for f in csv_files:
        try:
            frames.append(pd.read_csv(f))
        except Exception:
            continue
    if not frames:
        return None, 0
    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["sentiment", "timestamp"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    return df, len(csv_files)

# ── Header ───────────────────────────────────────────────────────────────────
st.title("📊 Real-Time Social Media Sentiment Analytics")
st.caption(f"Big Data Platform — Spring 2026 | Rackhel Fernando L.B. (202312229) & Sheikh MD Sifat (202312254) | Auto-refreshes every {REFRESH_INTERVAL}s")

df, file_count = load_data()

if df is None or len(df) == 0:
    st.warning("⏳ No data yet. Start `tweet_producer.py` and `spark_consumer.py`, then wait ~10 seconds for first CSV output.")
    st.info("Pipeline: `tweet_producer.py` → Kafka → `spark_consumer.py` → `output/sentiment_results/`")
else:
    counts = df["sentiment"].value_counts()
    total  = len(df)
    pos    = counts.get("positive", 0)
    neg    = counts.get("negative", 0)
    neu    = counts.get("neutral",  0)

    # ── Metrics ──────────────────────────────────────────────────────────────
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("📨 Total Tweets", f"{total:,}")
    col2.metric("✅ Positive",  f"{pos:,}",  f"{pos/total*100:.1f}%")
    col3.metric("❌ Negative",  f"{neg:,}",  f"{neg/total*100:.1f}%")
    col4.metric("➖ Neutral",   f"{neu:,}",  f"{neu/total*100:.1f}%")
    col5.metric("📁 CSV Files", file_count)

    # ── Alert banner ─────────────────────────────────────────────────────────
    df["window"] = df["timestamp"].dt.floor("10s")
    latest_window = df["window"].max()
    recent_neg = df[(df["window"] == latest_window) & (df["sentiment"] == "negative")].shape[0]
    if recent_neg >= ALERT_THRESHOLD:
        st.error(
            f"🚨 **NEGATIVE SPIKE ALERT** — {recent_neg} negative tweets in the latest "
            f"10-second window (threshold: {ALERT_THRESHOLD})"
        )

    st.divider()

    # ── Charts row ───────────────────────────────────────────────────────────
    c1, c2 = st.columns(2)

    with c1:
        st.subheader("Overall Sentiment Distribution")
        bar_df = counts.reset_index()
        bar_df.columns = ["sentiment", "count"]
        fig_bar = px.bar(
            bar_df, x="sentiment", y="count",
            color="sentiment", color_discrete_map=COLORS,
            text="count",
            labels={"sentiment": "Sentiment", "count": "Tweet Count"},
        )
        fig_bar.update_traces(textposition="outside")
        fig_bar.update_layout(showlegend=False, margin=dict(t=20, b=10))
        st.plotly_chart(fig_bar, use_container_width=True)

    with c2:
        st.subheader("Sentiment Share (%)")
        fig_pie = px.pie(
            values=counts.values,
            names=[s.capitalize() for s in counts.index],
            color=counts.index,
            color_discrete_map={k.capitalize(): v for k, v in COLORS.items()},
            hole=0.35,
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+label")
        fig_pie.update_layout(margin=dict(t=20, b=10))
        st.plotly_chart(fig_pie, use_container_width=True)

    # ── Time series ──────────────────────────────────────────────────────────
    st.subheader("Sentiment Counts Over Time (10-second windows)")
    time_series = (
        df.groupby(["window", "sentiment"])
          .size()
          .reset_index(name="count")
    )
    fig_line = px.line(
        time_series, x="window", y="count", color="sentiment",
        color_discrete_map=COLORS, markers=True,
        labels={"window": "Time Window", "count": "Tweet Count", "sentiment": "Sentiment"},
    )
    fig_line.update_layout(margin=dict(t=20, b=10))
    st.plotly_chart(fig_line, use_container_width=True)

    # ── Recent tweets ─────────────────────────────────────────────────────────
    st.subheader("Recent Tweets")
    recent_tweets = df.sort_values("timestamp", ascending=False).head(20).copy()
    recent_tweets["sentiment"] = recent_tweets["sentiment"].map(
        {"positive": "✅ positive", "negative": "❌ negative", "neutral": "➖ neutral"}
    )
    st.dataframe(
        recent_tweets[["timestamp", "tweet", "sentiment"]].rename(
            columns={"timestamp": "Time", "tweet": "Tweet", "sentiment": "Sentiment"}
        ),
        use_container_width=True,
        hide_index=True,
    )

st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

# ── Auto-refresh: sleep then trigger a full Streamlit rerun ──────────────────
time.sleep(REFRESH_INTERVAL)
st.rerun()