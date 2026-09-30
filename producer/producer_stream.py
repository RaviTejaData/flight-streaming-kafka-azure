import json
import logging
import signal
import time

import requests
from confluent_kafka import Producer

# Part 1 - Settings and logging
API_URL = "https://api.adsb.lol/v2/point/50.04/8.56/50"
HEADERS = {"User-Agent": "streaming-lab/0.1 (https://github.com/RaviTejaData)"}
TOPIC = "flights.positions.raw"
POLL_SECONDS = 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("producer")

# CHANGED in Step 7: delivery guarantees
producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "partitioner": "murmur2_random",
    "enable.idempotence": True,     # no duplicates or reordering from retries
    "acks": "all",                  # wait for all in-sync copies to save it
    "delivery.timeout.ms": 120000,  # keep retrying for up to 2 minutes
    "compression.type": "zstd",     # CHANGED in Step 8: compress each batch
    "linger.ms": 100,               # CHANGED in Step 8: wait 100 ms to fill batches
})

# Part 2 - Stop flag, switched off by Ctrl+C or by "docker stop"
running = True

def request_stop(signum, frame):
    global running
    log.info("Stop requested (signal %s) - finishing up...", signum)
    running = False

signal.signal(signal.SIGINT, request_stop)
signal.signal(signal.SIGTERM, request_stop)

# Part 3 - Delivery report
results = {"ok": 0, "failed": 0}

def on_delivery(err, msg):
    if err is not None:
        results["failed"] += 1
        log.error("Delivery failed for key=%s: %s", msg.key(), err)
    else:
        results["ok"] += 1

# Part 4 - One cycle: fetch a snapshot, send each plane
def fetch_snapshot():
    response = requests.get(API_URL, headers=HEADERS, timeout=10)
    response.raise_for_status()
    return response.json()

def send_snapshot(snapshot):
    fetched_at_ms = int(time.time() * 1000)
    aircraft_list = snapshot.get("ac", [])
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
    return len(aircraft_list)

# Part 5 - Wait in small steps so a stop request is noticed quickly
def wait_or_stop(seconds):
    end = time.monotonic() + seconds
    while running and time.monotonic() < end:
        time.sleep(0.2)
        producer.poll(0)

# Part 6 - The main loop
log.info("Starting: polling every %s seconds. Press Ctrl+C to stop.", POLL_SECONDS)
while running:
    try:
        count = send_snapshot(fetch_snapshot())
        # CHANGED in Step 7: "Queued", because produce() only fills the waiting area
        log.info("Queued %s aircraft (delivered so far: %s, failed: %s)",
                 count, results["ok"], results["failed"])
    except requests.RequestException as error:
        log.warning("API call failed, will retry next cycle: %s", error)
    wait_or_stop(POLL_SECONDS)

# Part 7 - Clean shutdown
remaining = producer.flush(10)
log.info("Stopped. Delivered: %s  Failed: %s  Not delivered: %s",
         results["ok"], results["failed"], remaining)
