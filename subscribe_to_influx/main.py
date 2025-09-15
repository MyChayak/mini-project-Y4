from quixstreams import Application
from influxdb_client import InfluxDBClient, Point, WriteOptions
from datetime import datetime, timezone
import os
import json
from datetime import datetime
import logging

# Load environment variables (useful when working locally)
from dotenv import load_dotenv
# load_dotenv(os.path.dirname(os.path.abspath(__file__))+"/.env")
load_dotenv(".env")

# Logggin env
log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level_str, logging.INFO)

logging.basicConfig(
    level=log_level,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


# --- InfluxDB Setup ---
INFLUX_URL = os.getenv("INFLUX_URL", "http://influxdb86:8086")
INFLUX_TOKEN = os.getenv("INFLUX_TOKEN", "your_token")
INFLUX_ORG = os.getenv("INFLUX_ORG", "your_org")
INFLUX_BUCKET = os.getenv("INFLUX_BUCKET", "iot_data")

influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
influx_write = influx_client.write_api(write_options=WriteOptions(batch_size=1, flush_interval=1))

# --- Quix Setup ---
# Config
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "172.16.2.117:9092")
KAFKA_INPUT_TOPIC = os.getenv("KAFKA_INPUT_TOPIC", "event-frames-model")

app = Application(broker_address=KAFKA_BROKER,
                loglevel="INFO",
                auto_offset_reset="earliest",
                state_dir=os.path.dirname(os.path.abspath(__file__))+"/state/",
                consumer_group="model-influxdb"
      )
input_topic = app.topic(KAFKA_INPUT_TOPIC, value_deserializer="json")


def process_event(data):
    try:
        payload = data
        timestamp_ms = payload.get("timestamp_ms", None)
        timestamp = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc) if timestamp_ms else datetime.utcnow()
        logging.info(f"timestamp-{timestamp}")
        # logging.info(f"[📥] Got message: {data}")

        point = (
            Point(KAFKA_INPUT_TOPIC)
            # .tag("id", payload.get("id", "unknow"))           #.tag to be filter
            .field("T2", float(payload.get("T2", 0)))
            .field("T2_diff", float(payload.get("T2_diff", 0)))
            .field("T2_roll", float(payload.get("T2_roll", 0)))
            .field("T24", float(payload.get("T24", 0)))
            .field("T24_diff", float(payload.get("T24_diff", 0)))
            .field("T24_roll", float(payload.get("T24_roll", 0)))
            .field("T30", float(payload.get("T30", 0)))
            .field("T30_diff", float(payload.get("T30_diff", 0)))
            .field("T30_roll", float(payload.get("T30_roll", 0)))
            .field("T50", float(payload.get("T50", 0)))
            .field("T50_diff", float(payload.get("T50_diff", 0)))
            .field("T50_roll", float(payload.get("T50_roll", 0)))
            .field("P2", float(payload.get("P2", 0)))
            .field("P2_diff", float(payload.get("P2_diff", 0)))
            .field("P2_roll", float(payload.get("P2_roll", 0)))
            .field("P15", float(payload.get("P15", 0)))
            .field("P15_diff", float(payload.get("P15_diff", 0)))
            .field("P15_roll", float(payload.get("P15_roll", 0)))
            .field("P30", float(payload.get("P30", 0)))
            .field("P30_diff", float(payload.get("P30_diff", 0)))
            .field("P30_roll", float(payload.get("P30_roll", 0)))
            .field("Nf", float(payload.get("Nf", 0)))
            .field("Nf_diff", float(payload.get("Nf_diff", 0)))
            .field("Nf_roll", float(payload.get("Nf_roll", 0)))
            .field("Nc", float(payload.get("Nc", 0)))
            .field("Nc_diff", float(payload.get("Nc_diff", 0)))
            .field("Nc_roll", float(payload.get("Nc_roll", 0)))
            .field("epr", float(payload.get("epr", 0)))
            .field("epr_diff", float(payload.get("epr_diff", 0)))
            .field("epr_roll", float(payload.get("epr_roll", 0)))
            .field("Ps30", float(payload.get("Ps30", 0)))
            .field("Ps30_diff", float(payload.get("Ps30_diff", 0)))
            .field("Ps30_roll", float(payload.get("Ps30_roll", 0)))
            .field("phi", float(payload.get("phi", 0)))
            .field("phi_diff", float(payload.get("phi_diff", 0)))
            .field("phi_roll", float(payload.get("phi_roll", 0)))
            .field("NRf", float(payload.get("NRf", 0)))
            .field("NRf_diff", float(payload.get("NRf_diff", 0)))
            .field("NRf_roll", float(payload.get("NRf_roll", 0)))
            .field("NRc", float(payload.get("NRc", 0)))
            .field("NRc_diff", float(payload.get("NRc_diff", 0)))
            .field("NRc_roll", float(payload.get("NRc_roll", 0)))
            .field("BPR", float(payload.get("BPR", 0)))
            .field("BPR_diff", float(payload.get("BPR_diff", 0)))
            .field("BPR_roll", float(payload.get("BPR_roll", 0)))
            .field("farB", float(payload.get("farB", 0)))
            .field("farB_diff", float(payload.get("farB_diff", 0)))
            .field("farB_roll", float(payload.get("farB_roll", 0)))
            .field("htBleed", float(payload.get("htBleed", 0)))
            .field("htBleed_diff", float(payload.get("htBleed_diff", 0)))
            .field("htBleed_roll", float(payload.get("htBleed_roll", 0)))
            .field("Nf_dmd", float(payload.get("Nf_dmd", 0)))
            .field("Nf_dmd_diff", float(payload.get("Nf_dmd_diff", 0)))
            .field("Nf_dmd_roll", float(payload.get("Nf_dmd_roll", 0)))
            .field("PCNfR_dmd", float(payload.get("PCNfR_dmd", 0)))
            .field("PCNfR_dmd_diff", float(payload.get("PCNfR_dmd_diff", 0)))
            .field("PCNfR_dmd_roll", float(payload.get("PCNfR_dmd_roll", 0)))
            .field("W31", float(payload.get("W31", 0)))
            .field("W31_diff", float(payload.get("W31_diff", 0)))
            .field("W31_roll", float(payload.get("W31_roll", 0)))
            .field("W32", float(payload.get("W32", 0)))
            .field("W32_diff", float(payload.get("W32_diff", 0)))
            .field("W32_roll", float(payload.get("W32_roll", 0)))
            .field("setting1", float(payload.get("setting1", 0)))
            .field("setting2", float(payload.get("setting2", 0)))
            .field("setting3", float(payload.get("setting3", 0)))
            .field("RUL", float(payload.get("RUL", 0)))
            .time(timestamp)
        )

        influx_write.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        logging.info(f"[✓] Wrote to InfluxDB: {point.to_line_protocol()}")

    except Exception as e:
        logging.error(f"❌ Error processing message: {e}")



# Stream
sdf = app.dataframe(input_topic)
sdf = sdf.apply(process_event)

logging.info(f"Connecting to ...{KAFKA_BROKER}")
logging.info(f"🚀 Listening to Kafka topic: {KAFKA_INPUT_TOPIC}")
app.run()
