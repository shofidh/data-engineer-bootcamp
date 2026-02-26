import json
from confluent_kafka import Consumer, KafkaException
from collections import defaultdict

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "events_group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # manual commit
}

TOPIC_NAME = "events_topic"


def print_assignment(consumer, partitions):
    print("📌 Partition Assigned:")

    for p in partitions:
        print(
            f"   Topic: {p.topic} | "
            f"Partition: {p.partition}"
        )

def start_consumer():
    consumer = Consumer(KAFKA_CONFIG)

    consumer.subscribe([TOPIC_NAME], on_assign=print_assignment)

    event_counter = defaultdict(int)

    print("🎧 Consumer berjalan...\n")

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                raise KafkaException(msg.error())

            key = msg.key().decode()
            value = json.loads(msg.value().decode())

            event_counter[key] += 1

            print(
                f"📥 Partition: {msg.partition()} | "
                f"Offset: {msg.offset()} | "
                f"Key: {key}"
            )
            print(f"   Data: {value}")
            print(f"   Total Event Per User: {dict(event_counter)}")
            print("-" * 50)

            consumer.commit(msg)

    except KeyboardInterrupt:
        print("\n🛑 Consumer dihentikan")

    finally:
        consumer.close()
        print("✅ Consumer ditutup")


if __name__ == "__main__":
    start_consumer()