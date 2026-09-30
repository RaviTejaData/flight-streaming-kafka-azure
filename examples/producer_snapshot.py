import json
import time

import requests
from confluent_kafka import Producer

# Part 1 - Settings
API_URL = "https://api.adsb.lol/v2/point/50.04/8.56/50"
HEADERS = {"User-Agent": "streaming-lab/0.1 (https://github.com/RaviTejaData)"}
TOPIC = "flights.positions.raw"

producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "partitioner": "murmur2_random",
})

# Part 2 - Delivery report: count results instead of printing every message
results = {"ok": 0, "failed": 0}

def on_delivery(err, msg):
    if err is not None:
        results["failed"] += 1
        print(f"FAILED key={msg.key()}: {err}")
    else:
        results["ok"] += 1

# Part 3 - Take one snapshot from the API
response = requests.get(API_URL, headers=HEADERS, timeout=10)
response.raise_for_status()
snapshot = response.json()

aircraft_list = snapshot.get("ac", [])
fetched_at_ms = int(time.time() * 1000)
print(f"Snapshot contains {len(aircraft_list)} aircraft")

# Part 4 - Turn each plane into one event
for aircraft in aircraft_list:
    event = {
        "source": "adsb.lol",
        "api_now_ms": snapshot.get("now"),
        "fetched_at_ms": fetched_at_ms,
        "aircraft": aircraft,
    }
    producer.produce(
        topic=TOPIC,
        key=aircraft["hex"],
        value=json.dumps(event),
        on_delivery=on_delivery,
    )
    producer.poll(0)

# Part 5 - Wait for everything to be delivered, then report
producer.flush()
print(f"Delivered: {results['ok']}  Failed: {results['failed']}")
