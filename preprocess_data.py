from detector import train_model_for_sensor, detect_anomalies_isolation_forest
import pandas as pd
from logger_config import setup_logger
from config import OUTDOOR_TEMP_TAG

# Internal buffer to hold CSP and PV payloads per tag
message_buffer = {}
logger = setup_logger(__name__)

def try_merge_and_detect(df_sp, df_pv, tag_name, mode, topic_name=None, outdoor_df=None):
    # Reset index to make 'Timestamp' a column
    df_sp = df_sp.rename(columns={"Value": f"SetPoint_{tag_name}_CSP"}).reset_index()
    df_pv = df_pv.rename(columns={"Value": f"Actual_{tag_name}_PV"}).reset_index()

    # Ensure Timestamp is datetime
    df_sp["Timestamp"] = pd.to_datetime(df_sp["Timestamp"])
    df_pv["Timestamp"] = pd.to_datetime(df_pv["Timestamp"])

    # Sort by Timestamp
    df_sp = df_sp.sort_values("Timestamp")
    df_pv = df_pv.sort_values("Timestamp")

    # Merge with tolerance
    df = pd.merge_asof(
        df_sp, df_pv, on="Timestamp",
        direction="nearest",
        tolerance=pd.Timedelta("300s")  # Increased from 300s to allow possibility of mismatch of one data point in CSP and PV
    )

    logger.info(f"[{tag_name}] Merge done. Total rows: {len(df)}. NaN rows (any): {df.isna().any(axis=1).sum()}")

    # Merge the outdoor temperature with relevant data 
    if topic_name and "heating" in topic_name.lower():
        if OUTDOOR_TEMP_TAG and outdoor_df is not None and not outdoor_df.empty:
            outdoor_df = outdoor_df.rename(columns={"Value": "Outdoor_Temperature"}).reset_index()
            outdoor_df["Timestamp"] = pd.to_datetime(outdoor_df["Timestamp"])
            outdoor_df = outdoor_df.sort_values("Timestamp")
            df = pd.merge_asof(df, outdoor_df, on="Timestamp", direction="nearest", tolerance=pd.Timedelta("60s"))
            logger.info(f"[{tag_name}] Outdoor temp merged. NaN in Outdoor_Temperature: {df['Outdoor_Temperature'].isna().sum()}")
            logger.info(f"[{tag_name}] Merged outdoor temperature: {OUTDOOR_TEMP_TAG}")
        else:
            logger.info(f"[{tag_name}] No outdoor temperature payload found for: {OUTDOOR_TEMP_TAG}")


    # Drop rows where both CSP and PV are missing
    before_drop = len(df)
    df = df.dropna(subset=[f"SetPoint_{tag_name}_CSP", f"Actual_{tag_name}_PV"], how='all')
    dropped_rows = before_drop - len(df)
    logger.info(f"[{tag_name}] Dropped rows with both NaNs: {dropped_rows}")
    
    # Interpolate only small gaps (limit=2 means max 2 rows filled in a gap)
    df = df.sort_values("Timestamp").interpolate(limit=2)

    # Fallback to fill single edge NaNs
    df = df.bfill(limit=1).ffill(limit=1)

    logger.info(f"[{tag_name}] Interpolation and fill done. Remaining NaNs: {df.isna().sum().sum()}")

    latest_row = None
    if mode == "historical":
        # Handling of Outside temperature value when topic is heating
        logger.info(f"Historical mode, using data to train model")
        train_model_for_sensor(df.copy(), f"{tag_name}_CSP", f"{tag_name}_PV", topic_name)       
        
    else:
        logger.info(f"Real time mode, using data to predict")
        df_anomaly, has_anomaly = detect_anomalies_isolation_forest(df.copy(), f"{tag_name}_CSP", f"{tag_name}_PV")
        logger.info(f"Anomaly detection completed for tag: {tag_name}")
        latest_row = df_anomaly[df_anomaly["Timestamp"] == df_anomaly["Timestamp"].max()]
        logger.info(f"Detection done for pair: {tag_name}, anomalies: {has_anomaly}")

    if tag_name in message_buffer:
        del message_buffer[tag_name]
        logger.info(f"Cleared buffer for tag prefix: {tag_name}")

    return latest_row