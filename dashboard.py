"""
Real-Time Sentiment Dashboard — Streamlit (Enhanced)
Reads from output/sentiment_results/*.csv and auto-refreshes every 5 seconds.

New in final version:
  - Topic breakdown bar chart (tech / sports / weather / food / work)
  - Compound score distribution histogram
  - Sentiment × Topic heatmap table

Run with: streamlit run dashboard.py
"""

import glob
import time
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Sentiment Analytics Dashboard",
    page_icon="📊",
    layout="wide",
)

REFRESH_INTERVAL = 5   # seconds
ALERT_THRESHOLD  = 10

COLORS = {
    "positive": "#2ecc71",
    "negative": "#e74c3c",
    "neutral":  "#95a5a6",
}

TOPIC_COLORS = {
    "tech":    "#3498db",
    "sports":  "#e67e22",
    "weather": "#1abc9c",
    "food":    "#9b59b6",
    "work":    "#f39c12",
}

# ── Load data ─────────────────────────────────────────────────────────────────
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
    # Coerce compound_score if present
    if "compound_score" in df.columns:
        df["compound_score"] = pd.to_numeric(df["compound_score"], errors="coerce")
    # Fill missing topic (backward compat with old CSVs)
    if "topic" not in df.columns:
        df["topic"] = "unknown"
    df["topic"] = df["topic"].fillna("unknown")
    return df, len(csv_files)

# ── Header ────────────────────────────────────────────────────────────────────
st.title("📊 Real-Time Social Media Sentiment Analytics")
st.caption(
    f"Big Data Platform — Spring 2026 | "
    f"Rackhel Fernando L.B. (202312229) & Sheikh MD Sifat (202312254) | "
    f"Auto-refreshes every {REFRESH_INTERVAL}s"
)

df, file_count = load_data()

if df is None or len(df) == 0:
    st.warning("⏳ No data yet. Start `tweet_producer.py` and `spark_consumer.py`, then wait ~10 seconds.")
    st.info("Pipeline: `tweet_producer.py` → Kafka → `spark_consumer.py` → `output/sentiment_results/`")
else:
    counts = df["sentiment"].value_counts()
    total  = len(df)
    pos    = counts.get("positive", 0)
    neg    = counts.get("negative", 0)
    neu    = counts.get("neutral",  0)

    # ── KPI metrics ───────────────────────────────────────────────────────────
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("📨 Total Tweets",   f"{total:,}")
    col2.metric("✅ Positive",       f"{pos:,}",  f"{pos/total*100:.1f}%")
    col3.metric("❌ Negative",       f"{neg:,}",  f"{neg/total*100:.1f}%")
    col4.metric("➖ Neutral",        f"{neu:,}",  f"{neu/total*100:.1f}%")
    col5.metric("📁 CSV Files",      file_count)
    if "compound_score" in df.columns and df["compound_score"].notna().any():
        avg_score = df["compound_score"].mean()
        col6.metric("📈 Avg Compound", f"{avg_score:+.3f}")

    # ── Alert banner ──────────────────────────────────────────────────────────
    df["window"] = df["timestamp"].dt.floor("10s")
    latest_window = df["window"].max()
    recent_neg = df[(df["window"] == latest_window) & (df["sentiment"] == "negative")].shape[0]
    if recent_neg >= ALERT_THRESHOLD:
        st.error(
            f"🚨 **NEGATIVE SPIKE ALERT** — {recent_neg} negative tweets in the latest "
            f"10-second window (threshold: {ALERT_THRESHOLD})"
        )

    st.divider()

    # ── Row 1: Sentiment distribution + share ─────────────────────────────────
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

    # ── Row 2: Topic breakdown + Compound score histogram ─────────────────────
    st.divider()
    c3, c4 = st.columns(2)

    with c3:
        st.subheader("Tweet Volume by Topic")
        topic_counts = df["topic"].value_counts().reset_index()
        topic_counts.columns = ["topic", "count"]
        fig_topic = px.bar(
            topic_counts, x="topic", y="count",
            color="topic", color_discrete_map=TOPIC_COLORS,
            text="count",
            labels={"topic": "Topic", "count": "Tweet Count"},
        )
        fig_topic.update_traces(textposition="outside")
        fig_topic.update_layout(showlegend=False, margin=dict(t=20, b=10))
        st.plotly_chart(fig_topic, use_container_width=True)

    with c4:
        if "compound_score" in df.columns and df["compound_score"].notna().any():
            st.subheader("Compound Score Distribution")
            fig_hist = px.histogram(
                df, x="compound_score", nbins=30,
                color="sentiment", color_discrete_map=COLORS,
                labels={"compound_score": "VADER Compound Score", "count": "Frequency"},
                barmode="overlay", opacity=0.75,
            )
            fig_hist.add_vline(x=0.05,  line_dash="dash", line_color="#2ecc71",
                               annotation_text="positive threshold")
            fig_hist.add_vline(x=-0.05, line_dash="dash", line_color="#e74c3c",
                               annotation_text="negative threshold")
            fig_hist.update_layout(margin=dict(t=20, b=10))
            st.plotly_chart(fig_hist, use_container_width=True)
        else:
            st.info("Compound score data not available in current CSV files.")

    # ── Row 3: Sentiment × Topic heatmap ──────────────────────────────────────
    st.divider()
    st.subheader("Sentiment × Topic Breakdown")
    known_topics = [t for t in ["tech", "sports", "weather", "food", "work"]
                    if t in df["topic"].values]
    if known_topics:
        pivot = df[df["topic"].isin(known_topics)] \
            .groupby(["topic", "sentiment"]).size() \
            .unstack(fill_value=0)
        # Ensure all sentiment columns present
        for s in ["positive", "negative", "neutral"]:
            if s not in pivot.columns:
                pivot[s] = 0
        pivot = pivot[["positive", "negative", "neutral"]]
        pivot["total"] = pivot.sum(axis=1)
        pivot_pct = pivot[["positive", "negative", "neutral"]].div(pivot["total"], axis=0).mul(100).round(1)

        fig_heat = go.Figure(data=go.Heatmap(
            z=pivot_pct.values,
            x=["Positive %", "Negative %", "Neutral %"],
            y=pivot_pct.index.str.capitalize(),
            colorscale="RdYlGn",
            text=pivot_pct.values,
            texttemplate="%{text}%",
            showscale=True,
            zmin=0, zmax=100,
        ))
        fig_heat.update_layout(
            margin=dict(t=20, b=10),
            xaxis_title="Sentiment",
            yaxis_title="Topic",
        )
        st.plotly_chart(fig_heat, use_container_width=True)

    # ── Row 4: Sentiment over time ────────────────────────────────────────────
    st.divider()
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

    # ── Row 5: Recent tweets ──────────────────────────────────────────────────
    st.divider()
    st.subheader("Recent Tweets")
    cols_to_show = ["timestamp", "topic", "tweet", "compound_score", "sentiment"]
    cols_present = [c for c in cols_to_show if c in df.columns]
    recent = df.sort_values("timestamp", ascending=False).head(20).copy()
    recent["sentiment"] = recent["sentiment"].map(
        {"positive": "✅ positive", "negative": "❌ negative", "neutral": "➖ neutral"}
    )
    rename_map = {
        "timestamp":     "Time",
        "topic":         "Topic",
        "tweet":         "Tweet",
        "compound_score":"Score",
        "sentiment":     "Sentiment",
    }
    st.dataframe(
        recent[cols_present].rename(columns=rename_map),
        use_container_width=True,
        hide_index=True,
    )

st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

# ── Auto-refresh ──────────────────────────────────────────────────────────────
time.sleep(REFRESH_INTERVAL)
st.rerun()