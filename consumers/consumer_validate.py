import json
import logging
import signal
import sys
import time

from confluent_kafka import Consumer, KafkaException, Producer

# Part 1 - Settings
TOPIC = "flights.positions.raw"
DLQ_TOPIC = "flights.positions.dlq"
NAME = sys.argv[1] if len(sys.argv) > 1 else "validator"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger(NAME)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "flights-validator",
    "client.id": NAME,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

dlq_producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "enable.idempotence": True,
    "acks": "all",
})

# Part 2 - Stop flag
running = True

def request_stop(signum, frame):
    global running
    log.info("Stop requested (signal %s) - finishing up...", signum)
    running = False

signal.signal(signal.SIGINT, request_stop)
signal.signal(signal.SIGTERM, request_stop)



# Part 2b - Join the group and ask for the topic (this was missing)
def on_assign(consumer, partitions):
    log.info(">>> Assigned partitions: %s", [p.partition for p in partitions])

consumer.subscribe([TOPIC], on_assign=on_assign)

# Part 3 - The rules a message must pass
def validate(raw_value):
    """Return the plane record if the message is usable, otherwise raise ValueError."""
    event = json.loads(raw_value)
    if not isinstance(event, dict):
        raise ValueError("message is not a JSON object")
    plane = event.get("aircraft")
    if not isinstance(plane, dict):
        raise ValueError("missing 'aircraft' envelope")
    if not plane.get("hex"):
        raise ValueError("missing aircraft id 'hex'")
    if not isinstance(plane.get("lat"), (int, float)) or not isinstance(plane.get("lon"), (int, float)):
        raise ValueError("missing or invalid position")
    if not isinstance(event.get("api_now_ms"), int):
        raise ValueError("missing snapshot time 'api_now_ms'")
    return plane

# Part 4 - Send a bad message to the DLQ, unchanged, with labels explaining why
dlq_errors = []

def on_dlq_delivery(err, msg):
    if err is not None:
        dlq_errors.append(err)

def send_to_dlq(msg, reason):
    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=msg.key(),
        value=msg.value(),
        headers=[
            ("error.reason", reason),
            ("source.topic", msg.topic()),
            ("source.partition", str(msg.partition())),
            ("source.offset", str(msg.offset())),
            ("failed.at.ms", str(int(time.time() * 1000))),
        ],
        on_delivery=on_dlq_delivery,
    )
    dlq_producer.poll(0)

# Part 5 - Main loop
good = 0
bad = 0
log.info("Validating %s -> bad messages go to %s", TOPIC, DLQ_TOPIC)
while running:
    messages = consumer.consume(num_messages=100, timeout=1.0)
    if not messages:
        continue

    batch_bad = 0
    for msg in messages:
        if msg.error():
            log.error("Kafka error: %s", msg.error())
            continue
        try:
            validate(msg.value())
            good += 1
        except (ValueError, TypeError) as error:
            send_to_dlq(msg, str(error))
            batch_bad += 1
            log.warning("-> DLQ  partition %s offset %s: %s",
                        msg.partition(), msg.offset(), error)

    # Safety rule: bad messages must be safely stored BEFORE we save the bookmark
    if batch_bad:
        remaining = dlq_producer.flush(10)
        if remaining > 0 or dlq_errors:
            log.error("DLQ write failed - stopping WITHOUT saving the bookmark")
            break
        bad += batch_bad

    try:
        consumer.commit(asynchronous=False)
    except KafkaException as error:
        log.warning("Commit failed (probably a rebalance) - these may be re-read: %s", error)

    log.info("Batch of %s done. Good so far: %s  Dead-lettered so far: %s",
             len(messages), good, bad)

# Part 6 - Clean shutdown
consumer.close()
log.info("Stopped. Good: %s  Dead-lettered: %s", good, bad)
