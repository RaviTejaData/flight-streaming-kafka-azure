#!/usr/bin/env bash
# Creates the Kafka topics this project needs. Safe to run more than once.
set -euo pipefail

KAFKA_TOPICS="docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"

$KAFKA_TOPICS --create --if-not-exists \
  --topic flights.positions.raw \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=259200000

$KAFKA_TOPICS --create --if-not-exists \
  --topic flights.positions.dlq \
  --partitions 1 --replication-factor 1 \
  --config retention.ms=1209600000

$KAFKA_TOPICS --list
