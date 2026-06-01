"""
附加题3-C2: 边缘计算模拟 - K3s + MQTT

模拟场景:
- 本地 K3s 作为边缘节点，运行传感器数据采集程序
- MQTT Broker 作为云边通信桥梁
- 云端 K8s 通过 MQTT 接收传感器数据并存入 Redis

运行方式:
  1. 本地 K3s: python k3s_mqtt_sensor.py --mode edge
  2. 云端 K8s: python k3s_mqtt_sensor.py --mode cloud

依赖: pip install paho-mqtt redis
"""
import paho.mqtt.client as mqtt
import redis
import json
import time
import random
import argparse
import os


MQTT_BROKER = os.environ.get("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", 1883))
MQTT_TOPIC = "sensors/temperature"
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))


def simulate_sensor():
    """Generate simulated temperature sensor data"""
    return {
        "device_id": f"sensor-{random.randint(1, 5):03d}",
        "temperature": round(20 + random.gauss(0, 2), 2),
        "humidity": round(50 + random.gauss(0, 5), 2),
        "timestamp": time.time()
    }


def run_edge():
    """Edge mode: collect sensor data and publish to MQTT"""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    print(f"[Edge] Publishing sensor data to {MQTT_TOPIC}...")
    try:
        while True:
            data = simulate_sensor()
            client.publish(MQTT_TOPIC, json.dumps(data))
            print(f"[Edge] Published: {json.dumps(data)}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("[Edge] Stopped.")
        client.loop_stop()
        client.disconnect()


def run_cloud():
    """Cloud mode: subscribe to MQTT and store in Redis"""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    def on_message(client, userdata, msg):
        data = json.loads(msg.payload)
        key = f"sensor:{data['device_id']}:{int(data['timestamp'])}"
        r.setex(key, 3600, json.dumps(data))
        print(f"[Cloud] Stored: {key} -> temp={data['temperature']}C, hum={data['humidity']}%")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC)
    print(f"[Cloud] Listening on {MQTT_TOPIC}...")
    client.loop_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["edge", "cloud"], default="edge")
    args = parser.parse_args()

    if args.mode == "edge":
        run_edge()
    else:
        run_cloud()
