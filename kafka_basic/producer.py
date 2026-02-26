import json
import time
import random
from confluent_kafka import Producer

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "acks": "all",  # memastikan durability
}

TOPIC_NAME = "events_topic"
USERS = ["user_1", "user_2", "user_3"]


def delivery_report(err, msg):
    if err:
        print(f"❌ Gagal kirim pesan: {err}")
    else:
        print(
            f"✅ Topic: {msg.topic()} | "
            f"Partition: {msg.partition()} | "
            f"Offset: {msg.offset()} | "
            f"Key: {msg.key().decode()}"
        )


def start_producer():
    producer = Producer(KAFKA_CONFIG)
    print(f"🚀 Producer berjalan ke topic '{TOPIC_NAME}'...\n")

    index = 0

    try:
        while True:
            selected_user = USERS[index % len(USERS)]
            index += 1

            payload = {
                "timestamp": int(time.time()),
                "action": random.choice(["login", "click", "purchase"]),
                "amount": random.randint(100, 500),
            }

            producer.produce(
                topic=TOPIC_NAME,
                key=selected_user,
                value=json.dumps(payload),
                callback=delivery_report,
            )

            producer.poll(0)
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n🛑 Producer dihentikan")

    finally:
        print("⏳ Menunggu semua pesan terkirim...")
        producer.flush()
        print("✅ Producer selesai")


if __name__ == "__main__":
    start_producer()