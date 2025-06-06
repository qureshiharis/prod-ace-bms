import os
import asyncio
import pandas as pd
import pytz
import collections
import time

from avassa_client import approle_login
from avassa_client.volga import Consumer, Topic, CreateOptions, Position
from datetime import datetime, timezone
from helper import parse_and_store_payload
from logger_config import setup_logger

logger = setup_logger(__name__)

max_rows = int(os.getenv("DATA_POINTS_SAVED", 20))  # default to 20 if not set
write_delay = int(os.getenv("WRITE_DELAY", 20))  # default to 20 if not set

# Utility to flatten payloads
def process_payload(payload):
    processed = {}
    for key, value in payload.items():
        if isinstance(value, dict) and len(value) == 1:
            processed[key] = list(value.values())[0]
        else:
            processed[key] = value
    return processed

async def consume_topic(topic_name, session):
    try:
        topic = Topic.local(topic_name)
        async with Consumer(
            session=session,
            topic=topic,
            consumer_name=f"csv-writer-{topic_name}",
            mode='exclusive',
            position=Position.end(),
            on_no_exists=CreateOptions.wait()
        ) as consumer:
            await consumer.more(1)
            logger.info(f"Listening on {topic_name}")
            message_queue = collections.deque()
            first_message_time = None
            while True:
                try:
                    msg = await asyncio.wait_for(consumer.recv(), timeout=2)
                    msg.setdefault("remain", 0)
                except asyncio.TimeoutError:
                    msg = None

                if msg:
                    logger.info(f"Received raw message on {topic_name}")
                    payload = msg["payload"]
                    mode = payload.get("mode", "realtime")  # default to 'realtime' if not present
                    if mode == "historical":
                        logger.info(f"Received historical data for topic: {topic_name}")
                        parse_and_store_payload(topic_name, payload, mode)
                    
                    else:                        
                        parsed_payload = parse_and_store_payload(topic_name, payload, mode)
                        if parsed_payload is not None:

                            processed = process_payload(parsed_payload)
                            timestamp = processed.get("Timestamp")
                            try:
                                time_only = pd.to_datetime(int(timestamp.iloc[0])).strftime("%H:%M:%S")
                            except:
                                time_only = pd.Timestamp.now().strftime("%H:%M:%S")
                            
                            outdoor_temp = None
                            if "heating" in topic_name.lower() and "Outdoor_Temperature" in processed:
                                outdoor_temp = float(processed["Outdoor_Temperature"].iloc[0])

                            sensor_data = {}
                            for key, value in processed.items():
                                if key == "Outdoor_Temperature":
                                    continue  # Already handled separately
                                if "_" in key:
                                    prefix, *mid, suffix = key.split("_")
                                    parts = key.split("_")
                                    sensor_id = "_".join(parts[1:-1])
                                    entry = sensor_data.setdefault(sensor_id, {
                                        "Timestamp": timestamp.iloc[0] if hasattr(timestamp, "iloc") else timestamp,
                                        "TimeOnly": time_only,
                                        "Sensor": sensor_id,
                                        "SetPoint": None,
                                        "Actual": None,
                                        "Error": None,
                                        "Anomaly": None
                                    })
                                    if prefix == "SetPoint":
                                        entry["SetPoint"] = float(value.iloc[0])
                                    elif prefix == "Actual":
                                        entry["Actual"] = float(value.iloc[0])
                                    elif prefix == "Error":
                                        entry["Error"] = float(value.iloc[0])
                                    elif prefix == "Anomaly":
                                        entry["Anomaly"] = str(value).lower() in ["true", "1", "yes"]
                            if outdoor_temp is not None:
                                for entry in sensor_data.values():
                                    entry["Outdoor_Temperature"] = outdoor_temp


                            if not message_queue and first_message_time is None:
                                logger.info(f"{topic_name} queue is empty, setting first message time")
                                first_message_time = time.time()
                            message_queue.extend(sensor_data.values())
                            logger.info(f"{topic_name} queue length: {len(message_queue)}")

                # Always check for flush
                if first_message_time is not None and message_queue:
                    time_since_first_message = time.time() - first_message_time

                    if time_since_first_message >= write_delay:
                        df_new = pd.DataFrame(message_queue)
                        message_queue.clear()

                        file = topic_name + ".csv"
                        logger.info(f"{topic_name} flushing {len(df_new)} rows to CSV")
                        try:
                            if os.path.exists(file):
                                df_existing = pd.read_csv(file)
                                df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                                logger.info(f"Total rows in {file} before combining: {len(df_existing)}")
                            else:
                                df_combined = df_new

                            
                            if len(df_combined) > max_rows:
                                df_combined = df_combined.tail(max_rows)

                            df_combined.to_csv(file, index=False)
                            first_message_time = None
                            logger.info(f"Wrote {len(df_new)} new rows to {file}, keeping last {max_rows} rows.")
                        except Exception as e:
                            logger.error(f"Failed to write or trim {file}: {e}", exc_info=True)

    except Exception as e:
        logger.error(f"Error consuming {topic_name}: {e}", exc_info=True)

async def main():
    role_id = os.getenv("ROLE_ID")
    secret_id = os.getenv("SECRET_ID")

    try:
        session = approle_login(
            host="https://api.internal:4646",
            role_id=role_id,
            secret_id=secret_id
        )
        logger.info("Logged into Avassa successfully.")
    except Exception as e:
        logger.error(f"Login failed: {e}")
        return

    topics_env = os.getenv("TOPICS_TO_CONSUME", "")
    topic_names = [t.strip() for t in topics_env.split(",") if t.strip()]
    consumers = [consume_topic(name, session) for name in topic_names]
    await asyncio.gather(*consumers)

if __name__ == "__main__":
    asyncio.run(main())