# dashboard.py

import os
import pandas as pd
import altair as alt
import streamlit as st
from PIL import Image
import streamlit_authenticator as stauth
import base64

# Utility function to encode images to base64
def encode_image_to_base64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode()


st.set_page_config(page_title="Anomaly Dashboard", layout="wide")

# Set up credentials
credentials = {
    "usernames": {
        "acecybersafe": {
            "name": "Admin Ace",
            "password": "$2b$12$KLr6X08pCFrJE/sJm28FvuB09YJ0JHLx8wMeqbHb4t7Q5ICZQmnk."
        }
    }
}

#
# Initialize the authenticator
authenticator = stauth.Authenticate(
    credentials,
    "ace_dashboard_cookie",
    "signature_key",  # This should be a secure random string
    cookie_expiry_days=1
)

# Always call login and store auth status
left_col, center_col, right_col = st.columns([1, 2, 1])
with center_col:
    authenticator.login(location='main', fields={'Form name': 'Login'})
auth_status = st.session_state.get("authentication_status")


ace_building_b64 = encode_image_to_base64("ace_building.png")

# Access authentication status from session state
if auth_status is False:
    st.error("Username/password is incorrect")

    st.markdown(f"""
        <style>
        .stApp {{
            background: url("data:image/png;base64,{ace_building_b64}") no-repeat center center fixed;
            background-size: cover;
        }}
        </style>
    """, unsafe_allow_html=True)

    # Re-show background and logos
    st.markdown(f"""
        <style>
        .login-background-img {{
            position: fixed;
            top: 0;
            left: 0;
            width: 100vw;
            height: 100vh;
            z-index: -1;
            opacity: 0.5;
            object-fit: cover;
        }}
        </style>
        <img class="login-background-img" src="data:image/png;base64,{ace_building_b64}">
    """, unsafe_allow_html=True)

    ace_logo_b64 = encode_image_to_base64("ace_logo.png")
    ltu_logo_b64 = encode_image_to_base64("ltu_logo.png")
    sk_logo_b64 = encode_image_to_base64("skelleftea_kraft_logo.png")
    kommun_logo_b64 = encode_image_to_base64("skelleftea_kommun_logo.png")
    vinnova_b64 = encode_image_to_base64("vinnova.png")

    st.markdown(f"""
        <hr style="margin-top: 4rem;">
        <div style="text-align: center; font-size: 0.9rem; color: #555;">
            <p>The Arctic Center of Energy (ACE) is a global competence center that accelerates the electrification of society.<br>
            Through research and education we develop the knowledge and skills needed to succeed with the sustainable energy transition.</p>
            <br>
            <strong>Core partners</strong><br><br>
            <div style="display: flex; justify-content: center; align-items: center; gap: 40px;">
                <img src="data:image/png;base64,{ace_logo_b64}" height="50">
                <img src="data:image/png;base64,{ltu_logo_b64}" height="50">
                <img src="data:image/png;base64,{sk_logo_b64}" height="50">
                <img src="data:image/png;base64,{kommun_logo_b64}" height="50">
                <img src="data:image/png;base64,{vinnova_b64}" height="50">
            </div>
        </div>
    """, unsafe_allow_html=True)
    st.stop()
elif auth_status is None:


    # Display navigation bar
    st.markdown("""
        <div style='text-align: center; margin-top: 1rem; margin-bottom: 2rem; font-size: 1rem;'>
            <a href='https://en.acecybersafe.se/' target='_blank' style='margin: 0 20px; text-decoration: none; color: #0366d6;'>Project Website</a>
            <a href='https://acecybersafe.b2.avassa.net/' target='_blank' style='margin: 0 20px; text-decoration: none; color: #0366d6;'>Control Tower</a>
            <a href='https://arcticcenterofenergy.se/en/ace-house/' target='_blank' style='margin: 0 20px; text-decoration: none; color: #0366d6;'>ACE House</a>
        </div>
    """, unsafe_allow_html=True)

    # Add background image to the entire Streamlit app using .stApp
    st.markdown(f"""
        <style>
        .stApp {{
            background: url("data:image/png;base64,{ace_building_b64}") no-repeat center center fixed;
            background-size: cover;
        }}
        </style>
    """, unsafe_allow_html=True)



    # --- Partner banner for login screen ---
    ace_logo_b64 = encode_image_to_base64("ace_logo.png")
    ltu_logo_b64 = encode_image_to_base64("ltu_logo.png")
    sk_logo_b64 = encode_image_to_base64("skelleftea_kraft_logo.png")
    kommun_logo_b64 = encode_image_to_base64("skelleftea_kommun_logo.png")
    vinnova_b64 = encode_image_to_base64("vinnova.png")

    st.markdown(f"""
        <hr style="margin-top: 4rem;">
        <div style="text-align: center; font-size: 0.9rem; color: #555;">
            <p>The Arctic Center of Energy (ACE) is a global competence center that accelerates the electrification of society.<br>
            Through research and education we develop the knowledge and skills needed to succeed with the sustainable energy transition.</p>
            <br>
            <strong>Core partners</strong><br><br>
            <div style="display: flex; justify-content: center; align-items: center; gap: 40px;">
                <img src="data:image/png;base64,{ace_logo_b64}" height="50">
                <img src="data:image/png;base64,{ltu_logo_b64}" height="50">
                <img src="data:image/png;base64,{sk_logo_b64}" height="50">
                <img src="data:image/png;base64,{kommun_logo_b64}" height="50">
                <img src="data:image/png;base64,{vinnova_b64}" height="50">
            </div>
        </div>
    """, unsafe_allow_html=True)
    st.stop()
else:
    name = st.session_state.get("name")
    username = st.session_state.get("username")
    # Proceed with the rest of your application

@st.cache_data(ttl=0, show_spinner=False)
def get_file_last_modified_time(file_path):
    try:
        return os.path.getmtime(file_path)
    except FileNotFoundError:
        return 0
    
# --- Streamlit UI setup ---
st.title("Real-Time Anomaly Detection Dashboard")

st.markdown("""
Welcome to the **ACE Subsystem Monitoring Dashboard**.  
Use the sidebar to select a subsystem. The dashboard will display recent readings, anomalies, and sensor stats.
""")

logo_path = "logo.png"
if os.path.exists(logo_path):
    logo = Image.open(logo_path)
    st.sidebar.image(logo, use_container_width=True)

# --- Subsystems ---
st.sidebar.markdown("### Subsystems")

# Get the comma-separated topic list from environment variable
topic_env = os.getenv("TOPICS_TO_CONSUME", "anomalies-heating, anomalies-ventilation")
subsystems = [t.strip() for t in topic_env.split(",") if t.strip()]
subsystem = st.sidebar.selectbox("Select Subsystem", subsystems)

# --- Dashboard Controls ---
st.sidebar.markdown("### Dashboard Controls")
show_anomalies_only = st.sidebar.checkbox("Show Anomalous Sensors Only", value=False)
enable_refresh = st.sidebar.checkbox("Auto-refresh", value=True)

st.sidebar.markdown("### Stats")

csv_path = f"{subsystem}.csv"

if not os.path.exists(csv_path):
    st.warning(f"No data found for {subsystem}. Waiting for new data...")
    st.stop()

from streamlit_autorefresh import st_autorefresh
# File change detection
last_mod_time = get_file_last_modified_time(csv_path)
# Refresh only if the file has changed since last refresh
if enable_refresh:
    st_autorefresh(interval=2000, limit=1000, key=str(last_mod_time))

try:
    df = pd.read_csv(csv_path)
    
except Exception as e:
    st.error(f"Failed to read {csv_path}: {e}")
    st.stop()

# --- Process Data ---
df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")
df["Timestamp"] = df["Timestamp"].dt.tz_convert("Europe/Stockholm") if df["Timestamp"].dt.tz else df["Timestamp"].dt.tz_localize("UTC").dt.tz_convert("Europe/Stockholm")

df = df.dropna(subset=["Sensor", "SetPoint", "Actual", "Timestamp"]).sort_values("Timestamp")

sensors = sorted(df["Sensor"].unique())

st.sidebar.metric("Sensors Monitored", len(sensors))

if df.empty:
    st.info("No data available.")
    st.stop()

# --- Visualize each sensor ---
for sensor_id in sensors:
    sensor_df = df[df["Sensor"] == sensor_id].tail(10)

    if sensor_df.empty:
        continue

    latest_row = sensor_df.iloc[-1]
    if show_anomalies_only and not latest_row.get("Anomaly", False):
        continue

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

    base = alt.Chart(melted).mark_line(clip=False).encode(
        x=alt.X("Timestamp:T", title="Time", axis=alt.Axis(format="%H:%M",tickCount=5)),
        y=alt.Y("Value:Q", title="Value", scale=y_scale),
        color=alt.Color("Type:N"),
        strokeDash=alt.StrokeDash("Type:N")
    ).properties(width=800).interactive(bind_y=False)

    line_chart = base.mark_line(interpolate='monotone').encode(
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

    st.markdown(f"### Sensor: `{sensor_id}`")
    st.altair_chart(chart, use_container_width=True)

# --- Historical Anomalies ---
df_anomaly = df[df.get("Anomaly", False) == True].sort_values("Timestamp", ascending=False)
st.sidebar.metric("Anomalies", len(df_anomaly))
# Display logout button and user info in the sidebar
st.sidebar.markdown(f"👋 Logged in as **{name}**")
authenticator.logout("Logout", location="sidebar")
st.markdown("## Historical Anomalies")

if not df_anomaly.empty:
    st.dataframe(df_anomaly[["Timestamp", "Sensor", "SetPoint", "Actual", "Error"]].head(20), use_container_width=True)
else:
    st.write("No anomalies recorded yet.")