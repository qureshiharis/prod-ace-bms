from logger_config import setup_logger
import pandas as pd
from datetime import datetime
from collections import defaultdict
from preprocess_data import try_merge_and_detect

# Buffer to temporarily store data until both CSP and PV arrive
payload_buffer = defaultdict(dict)

logger = setup_logger(__name__)


def parse_and_store_payload(topic, payload):
    tag_name = list(payload.keys())[0]
    time_series = payload[tag_name]
    prefix = tag_name.rsplit('_', 1)[0]

    logger.info(f"Received payload for tag: {tag_name} with {len(time_series)} entries.")

    df = pd.DataFrame(time_series.items(), columns=["Timestamp", "Value"])
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df.set_index("Timestamp", inplace=True)

    logger.info(f"Converted payload to DataFrame with shape: {df.shape}")

    if tag_name.endswith("_CSP"):
        payload_buffer[prefix]["CSP"] = df
    elif tag_name.endswith("_PV"):
        payload_buffer[prefix]["PV"] = df

    if "CSP" in payload_buffer[prefix] and "PV" in payload_buffer[prefix]:
        logger.info(f"Pair found for tag prefix: {prefix}. Proceeding to merge and detect.")
        sp_df = payload_buffer[prefix].pop("CSP")
        pv_df = payload_buffer[prefix].pop("PV")

        payload_yay = try_merge_and_detect(sp_df, pv_df, prefix)

        logger.info(f"Anomaly detection completed for tag: {prefix}")

        if not payload_buffer[prefix]:
            del payload_buffer[prefix]
            logger.info(f"Cleared buffer for tag prefix: {prefix}")

        return payload_yay