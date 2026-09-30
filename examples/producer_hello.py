from confluent_kafka import Producer

# Part 1 - Settings
config = {
    "bootstrap.servers": "localhost:9092",
    "partitioner": "murmur2_random",
}

# Part 2 - Create the producer
producer = Producer(config)

# Part 3 - What to do when Kafka replies
def on_delivery(err, msg):
    if err is not None:
        print(f"FAILED: {err}")
    else:
        print(f"Stored in partition {msg.partition()} at offset {msg.offset()}")

# Part 4 - Send one message
producer.produce(
    topic="flights.positions.raw",
    key="4246a2",
    value='{"hex":"4246a2","flight":"AZG785","alt_baro":23000,"lat":50.075,"lon":7.350}',
    on_delivery=on_delivery,
)

# Part 5 - Wait until it's really delivered
producer.flush()
