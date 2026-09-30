import json
import logging
import signal
import sys

from confluent_kafka import Consumer, KafkaException

# Part 1 - Settings
TOPIC = "flights.positions.raw"
NAME = sys.argv[1] if len(sys.argv) > 1 else "consumer"   # CHANGED in Step 10

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",   # CHANGED in Step 10
)
log = logging.getLogger(NAME)                                    # CHANGED in Step 10

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "flights-printer",
    "client.id": NAME,                                           # CHANGED in Step 10
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

# Part 2 - Stop flag
running = True

def request_stop(signum, frame):
    global running
    log.info("Stop requested (signal %s) - finishing up...", signum)
    running = False

signal.signal(signal.SIGINT, request_stop)
signal.signal(signal.SIGTERM, request_stop)

# Part 3 - Report partitions given AND taken away
def on_assign(consumer, partitions):
    log.info(">>> Assigned partitions: %s", [p.partition for p in partitions])

def on_revoke(consumer, partitions):                             # CHANGED in Step 10
    log.info("<<< Revoked partitions: %s", [p.partition for p in partitions])

consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

# Part 4 - Process one message (check it's valid; real work goes here later)
def process(msg):
    try:
        event = json.loads(msg.value())
        return event["aircraft"]
    except (ValueError, KeyError, TypeError):
        log.warning("Skipping malformed message at partition %s offset %s",
                    msg.partition(), msg.offset())
        return None

# Part 5 - Main loop: read a batch, process it, save the bookmark, log a summary
total = 0
while running:
    messages = consumer.consume(num_messages=100, timeout=1.0)
    if not messages:
        continue

    summary = {}                                                 # CHANGED in Step 10
    for msg in messages:
        if msg.error():
            log.error("Kafka error: %s", msg.error())
            continue
        process(msg)
        total += 1
        p = msg.partition()
        if p not in summary:
            summary[p] = [msg.offset(), msg.offset()]
        summary[p][1] = msg.offset()

    try:                                                         # CHANGED in Step 10
        consumer.commit(asynchronous=False)
    except KafkaException as error:
        log.warning("Commit failed (probably a rebalance) - these may be re-read: %s", error)

    parts = ", ".join(f"p{p} o{first}-o{last}" for p, (first, last) in sorted(summary.items()))
    log.info("Processed %s messages: %s", len(messages), parts)

# Part 6 - Clean shutdown
consumer.close()
log.info("Stopped. Read %s messages.", total)
