# dashboard.py

import os
import pandas as pd
import altair as alt
import streamlit as st
from datetime import datetime
import pytz

import time

@st.cache_data(ttl=0, show_spinner=False)
def get_file_last_modified_time(file_path):
    try:
        return os.path.getmtime(file_path)
    except FileNotFoundError:
        return 0
    
# --- Streamlit UI setup ---
st.set_page_config(page_title="Anomaly Dashboard", layout="wide")
st.title("Real-Time Anomaly Detection Dashboard")

st.markdown("""
Welcome to the **ACE Subsystem Monitoring Dashboard**.  
Use the sidebar to select a subsystem. The dashboard will display recent readings, anomalies, and sensor stats.
""")


topic_env = os.getenv("TOPICS_TO_CONSUME", "anomalies-heating,anomalies-ventilation")
subsystems = [t.strip().split("-", 1)[-1] for t in topic_env.split(",") if t.strip()]
subsystem = st.sidebar.selectbox("Select Subsystem", subsystems, format_func=str.title)

csv_path = f"{subsystem}.csv"

if not os.path.exists(csv_path):
    st.warning(f"No data file found for {subsystem}. Waiting for new data...")
    st.stop()

from streamlit_autorefresh import st_autorefresh
# File change detection
last_mod_time = get_file_last_modified_time(csv_path)
# Refresh only if the file has changed since last refresh
st_autorefresh(interval=2000, limit=1000, key=str(last_mod_time))

try:
    df = pd.read_csv(csv_path)
    
except Exception as e:
    st.error(f"Failed to read {csv_path}: {e}")
    st.stop()

# --- Process Data ---
try:
    df["Timestamp"] = pd.to_datetime(pd.to_numeric(df["Timestamp"], errors="coerce"), unit="ms").dt.tz_localize("UTC").dt.tz_convert("Europe/Stockholm")
except Exception:
    df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

df = df.dropna(subset=["Sensor", "SetPoint", "Actual", "Timestamp"]).sort_values("Timestamp")
sensors = sorted(df["Sensor"].unique())

if df.empty:
    st.info("No data available.")
    st.stop()

# --- Visualize each sensor ---
for sensor_id in sensors:
    sensor_df = df[df["Sensor"] == sensor_id].tail(10)

    if sensor_df.empty:
        continue

    latest_row = sensor_df.iloc[-1]
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Latest SetPoint", f"{latest_row['SetPoint']:.2f}")
    with col2:
        st.metric("Latest Actual", f"{latest_row['Actual']:.2f}")
    with col3:
        st.metric("Anomaly Detected", "Yes" if latest_row.get("Anomaly", False) else "No")

    y_min = sensor_df[["Actual", "SetPoint"]].min().min() - 2
    y_max = sensor_df[["Actual", "SetPoint"]].max().max() + 2
    y_scale = alt.Scale(domain=[y_min, y_max])

    melted = sensor_df.melt(
        id_vars=["Timestamp", "Sensor", "Error", "Anomaly"],
        value_vars=["Actual", "SetPoint"],
        var_name="Type",
        value_name="Value"
    )

    base = alt.Chart(melted).encode(
        x=alt.X("Timestamp:T", title="Time", axis=alt.Axis(format="%H:%M")),
        y=alt.Y("Value:Q", title="Value", scale=y_scale),
        color=alt.Color("Type:N"),
        strokeDash=alt.StrokeDash("Type:N")
    ).properties(width=800).interactive(bind_y=False)

    line_chart = base.mark_line().encode(
        tooltip=[alt.Tooltip("Timestamp:T", title="Time", format="%H:%M"), "Value:Q", "Anomaly:O"]
    )
    points = base.mark_point(filled=True).encode(
        color=alt.condition(
            alt.datum.Anomaly == True,
            alt.value('red'),  # Fill red if Anomaly is True
            alt.Color('Type:N')  # Otherwise color by Type
        ),
        size=alt.condition(
            alt.datum.Anomaly == True,
            alt.value(150),  # Bigger dot for anomaly
            alt.value(40)   # Normal dot size
        ),
        tooltip=[alt.Tooltip("Timestamp:T", title="Time", format="%H:%M"), "Value:Q", "Anomaly:O"]
    )
    anomaly_points = base.transform_filter(alt.datum.Anomaly == True).mark_point(color="red", size=75).encode(
        tooltip=[alt.Tooltip("Timestamp:T", title="Time", format="%H:%M"), "Value:Q", "Anomaly:O"]
    )

    chart = alt.layer(line_chart, points, anomaly_points)

    st.markdown(f"### 📡 Sensor: `{sensor_id}`")
    st.altair_chart(chart, use_container_width=True)

# --- Historical Anomalies ---
df_anomaly = df[df.get("Anomaly", False) == True].sort_values("Timestamp", ascending=False)
st.markdown("## 🔍 Historical Anomalies")

if not df_anomaly.empty:
    st.dataframe(df_anomaly[["Timestamp", "Sensor", "SetPoint", "Actual", "Error"]].head(20), use_container_width=True)
else:
    st.write("No anomalies recorded yet.")