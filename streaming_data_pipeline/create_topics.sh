#!/bin/bash

# create main topic
docker exec kafka-sdp kafka-topics \
--create \
--topic transactions \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1

# topic for valid data
docker exec kafka-sdp kafka-topics \
--create \
--topic transactions_valid \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1

# dead letter queue
docker exec kafka-sdp kafka-topics \
--create \
--topic transactions_dlq \
--bootstrap-server localhost:9092 \
--partitions 1 \
--replication-factor 1

echo "topics created"