from detector import detect_anomalies_for_pair
import pandas as pd
from logger_config import setup_logger

# Internal buffer to hold CSP and PV payloads per tag
message_buffer = {}
logger = setup_logger(__name__)

def try_merge_and_detect(df_sp, df_pv, tag_name):
    # Reset index to make 'Timestamp' a column
    df_sp = df_sp.rename(columns={"Value": f"SetPoint_{tag_name}_CSP"}).reset_index()
    df_pv = df_pv.rename(columns={"Value": f"Actual_{tag_name}_PV"}).reset_index()

    # Ensure Timestamp is datetime
    df_sp["Timestamp"] = pd.to_datetime(df_sp["Timestamp"])
    df_pv["Timestamp"] = pd.to_datetime(df_pv["Timestamp"])

    # Sort by Timestamp
    df_sp = df_sp.sort_values("Timestamp")
    df_pv = df_pv.sort_values("Timestamp")

    # Merge with 30s tolerance
    df = pd.merge_asof(df_sp, df_pv, on="Timestamp", direction="nearest", tolerance=pd.Timedelta("30s"))

    df = df.sort_values("Timestamp").interpolate().bfill().ffill()

    # Detect anomalies
    df_anomaly, has_anomaly = detect_anomalies_for_pair(df.copy(), f"{tag_name}_CSP", f"{tag_name}_PV")

    latest_row = df_anomaly[df_anomaly["Timestamp"] == df_anomaly["Timestamp"].max()]

    logger.info(f"Detection done for pair: {tag_name}, anomalies: {has_anomaly}")
    return latest_row