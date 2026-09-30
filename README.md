# Real-Time Flight Streaming Pipeline (Kafka → Azure)

Streams live aircraft positions from the free [adsb.lol](https://www.adsb.lol/) ADS-B API into Apache Kafka, validates every event, and routes broken messages to a dead-letter topic.

Built local-first. The Spark processing layer and the Azure deployment are in progress.

## Architecture (current)

```
adsb.lol API ──(poll every 10 s)──▶ producer ──▶ Kafka: flights.positions.raw (3 partitions)
                                                          │
                                                          ▼
                                              validating consumer ──▶ flights.positions.dlq
```

## What it demonstrates

- A polled API turned into an event stream: one event per aircraft, keyed by ICAO hex so each aircraft's positions stay in order
- An event envelope carrying snapshot and fetch timestamps, for event-time processing
- An idempotent producer (`acks=all`), tested to survive a broker restart with zero loss or duplication
- zstd compression and batching: messages ~76% smaller (828 → 198 bytes, measured)
- Graceful shutdown on SIGINT/SIGTERM, flushing all pending messages
- Consumer groups with manual commits after processing (at-least-once)
- Validation with a dead-letter topic: bad messages kept unchanged, with error and source headers

## Tech

Python · Apache Kafka 4.3 (KRaft) · Docker Compose · confluent-kafka

## Run it locally

```bash
docker compose up -d
./scripts/create_topics.sh
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python producer/producer_stream.py        # terminal 1
python consumers/consumer_validate.py     # terminal 2
```

## Roadmap

- Spark Structured Streaming → Delta Lake bronze / silver / gold
- Containerised producer with retries and backoff
- Azure: Event Hubs (Kafka endpoint), Databricks, ADLS Gen2, Key Vault, Terraform
