import numpy as np
import pandas as pd
import os
from logger_config import setup_logger
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
import joblib


logger = setup_logger(__name__)


from sklearn.ensemble import IsolationForest

ANOMALY_STD_MULTIPLIER = float(os.getenv("ANOMALY_STD_MULTIPLIER", 3))
ISF_CONTAMINATION = float(os.getenv("ISF_CONTAMINATION", 0.05))
ISF_RANDOM_STATE = int(os.getenv("ISF_RANDOM_STATE", 42))

def train_model_for_sensor(df: pd.DataFrame, sp_tag: str, pv_tag: str, topic_name: str):
    sp_col = f"SetPoint_{sp_tag}"
    pv_col = f"Actual_{pv_tag}"
    err_col = f"Error_{sp_tag}"

    df[err_col] = df[sp_col] - df[pv_col]

    df["Hour"] = df["Timestamp"].dt.hour
    df["DayOfWeek"] = df["Timestamp"].dt.dayofweek
    df["IsWeekend"] = df["DayOfWeek"].isin([5, 6]).astype(int)

    feature_cols = [sp_col, pv_col, err_col, "Hour", "DayOfWeek", "IsWeekend"]
    # Add Outdoor_Temperature only for heating topics and if present in columns
    if topic_name and "heating" in topic_name.lower() and "Outdoor_Temperature" in df.columns:
        feature_cols.append("Outdoor_Temperature")
    df_train = df[feature_cols].dropna()

    if df_train.empty:
        logger.info(f"No training data for sensor {sp_tag}, skipping.")
        return

    model = IsolationForest(contamination=ISF_CONTAMINATION, random_state=ISF_RANDOM_STATE)
    model.fit(df_train)

    model_filename = f"{sp_tag}_model.joblib"
    joblib.dump(model, model_filename)
    logger.info(f"Model trained and saved for {sp_tag} at {model_filename}")


def detect_anomalies_isolation_forest(df: pd.DataFrame, sp_tag: str, pv_tag: str, topic_name: str):

    sp_col = f"SetPoint_{sp_tag}"
    pv_col = f"Actual_{pv_tag}"
    err_col = f"Error_{sp_tag}"
    anomaly_col = f"Anomaly_{sp_tag}"

    df[err_col] = df[sp_col] - df[pv_col]
    df["Hour"] = df["Timestamp"].dt.hour
    df["DayOfWeek"] = df["Timestamp"].dt.dayofweek
    df["IsWeekend"] = df["DayOfWeek"].isin([5, 6]).astype(int)

    feature_cols = [sp_col, pv_col, err_col, "Hour", "DayOfWeek", "IsWeekend"]
    if topic_name and "heating" in topic_name.lower() and "Outdoor_Temperature" in df.columns:
        feature_cols.append("Outdoor_Temperature")
    df_predict = df[feature_cols].dropna()

    try:
        model_path = f"{sp_tag}_model.joblib"
        model = joblib.load(model_path)
    except FileNotFoundError:
        logger.warning(f"Model not found for {sp_tag}, skipping anomaly detection.")
        df[anomaly_col] = False
        return df, False

    preds = model.predict(df_predict)
    anomaly_flags = (preds == -1)

    # Align predictions with original DataFrame
    df[anomaly_col] = False
    df.loc[df_predict.index, anomaly_col] = anomaly_flags

    logger.info(f"Anomalies (Isolation Forest) detected for {sp_tag}: {anomaly_flags.sum()} rows")
    return df, anomaly_flags.any()

# Anomaly detection using Z-Score method 
def detect_anomalies_for_pair(df: pd.DataFrame, sp_tag: str, pv_tag: str) -> pd.DataFrame:
    sp_col = f"SetPoint_{sp_tag}"
    pv_col = f"Actual_{pv_tag}"
    err_col = f"Error_{sp_tag}"
    anomaly_col = f"Anomaly_{sp_tag}"

    if sp_col not in df.columns or pv_col not in df.columns:
        logger.warning(f"Missing required columns: {sp_col}, {pv_col}")
        return df

    df[err_col] = df[sp_col] - df[pv_col]

    mean = df[err_col].mean()
    std = df[err_col].std()
    threshold = ANOMALY_STD_MULTIPLIER * std

    df[anomaly_col] = np.abs(df[err_col] - mean) > threshold
    logger.info(f"Anomalies (Z-Score) detected for {sp_tag}: {df[anomaly_col].sum()} rows")

    return df, df[anomaly_col].any()