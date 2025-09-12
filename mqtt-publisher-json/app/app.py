import os
# from matplotlib.pyplot import bar_label
import ujson as json
import paho.mqtt.client as mqtt
import time

INPUT_JSON = os.getenv("INPUT_JSON", "train_reg.json")        # edit dataset file name
# MQTT_HOST  = os.getenv("MQTT_HOST", "172.16.2.117")
# MQTT_PORT  = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC="6510301013"
MQTT_HOST="vernemq1.local"
MQTT_PORT=1883

def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt-publisher-json")
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    with open(INPUT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    messages = data if isinstance(data, list) else [data]

    for msg in messages:
        data = {"name": "6510301013",
                "payload": msg}
        payload = json.dumps(data, ensure_ascii=False)
        print(payload)
        client.publish(MQTT_TOPIC, payload, qos=0, retain=False)
        time.sleep(0.5)
        print(f"Published → {MQTT_TOPIC}: {payload}")

    client.loop_stop()
    client.disconnect()

if __name__ == "__main__":
    main()
