import json
import random
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Connect to Kafka
# Producer runs on HOST machine → uses localhost:9092
# ---------------------------------------------------------------------------
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC   = "transactions"
SOURCES = ["mobile", "web", "pos"]

# Helper: timezone-aware UTC timestamp string
def now_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def ago_utc(minutes):
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------------------------------------------------------
# Valid event generator
# ---------------------------------------------------------------------------
def valid_event():
    return {
        "user_id":   f"U{random.randint(1000, 9999)}",
        "amount":    random.randint(1000, 500000),
        "timestamp": now_utc(),
        "source":    random.choice(SOURCES)
    }

# ---------------------------------------------------------------------------
# Invalid events — covers all 5 validation rules:
#   1. amount negatif
#   2. amount terlalu besar (> 10.000.000)
#   3. timestamp tidak valid
#   4. source tidak dikenal
#   5. user_id missing (null)
# ---------------------------------------------------------------------------
def invalid_events():
    return [
        # 1. amount negatif
        {"user_id": "U9991", "amount": -500,       "timestamp": now_utc(), "source": "mobile"},
        # 2. amount terlalu besar
        {"user_id": "U9992", "amount": 20_000_000, "timestamp": now_utc(), "source": "web"},
        # 3. timestamp tidak valid
        {"user_id": "U9993", "amount": 10000,      "timestamp": "INVALID_DATE", "source": "mobile"},
        # 4. source tidak dikenal
        {"user_id": "U9994", "amount": 75000,      "timestamp": now_utc(), "source": "atm"},
        # 5. missing user_id
        {"user_id": None,    "amount": 30000,      "timestamp": now_utc(), "source": "web"},
    ]

# ---------------------------------------------------------------------------
# Late events — minimal 3, semua > 3 menit (melebihi watermark)
# ---------------------------------------------------------------------------
def late_events():
    return [
        {"user_id": "U8881", "amount": 50000,  "timestamp": ago_utc(5),  "source": "mobile"},
        {"user_id": "U8882", "amount": 120000, "timestamp": ago_utc(7),  "source": "web"},
        {"user_id": "U8883", "amount": 95000,  "timestamp": ago_utc(10), "source": "pos"},
    ]

# ---------------------------------------------------------------------------
# Duplicate event — user_id + timestamp sama, dikirim 2x untuk trigger dedup
# ---------------------------------------------------------------------------
DUPLICATE_EVENT = {
    "user_id":   "U7771",
    "amount":    200000,
    "timestamp": "2025-03-06T10:00:00Z",   # fixed timestamp → always a duplicate
    "source":    "mobile"
}

# ---------------------------------------------------------------------------
# Main loop
# Probability: 65% valid | 15% invalid | 20% late | duplicate 2x di awal
# ---------------------------------------------------------------------------
duplicate_sent = 0

print(f"[Producer] Started → topic: {TOPIC}")
print("[Producer] Ctrl+C to stop.\n")

while True:
    r = random.random()

    if duplicate_sent < 2:
        event      = DUPLICATE_EVENT
        event_type = "DUPLICATE"
        duplicate_sent += 1

    elif r < 0.65:
        event      = valid_event()
        event_type = "VALID"

    elif r < 0.80:
        event      = random.choice(invalid_events())
        event_type = "INVALID"

    else:
        event      = random.choice(late_events())
        event_type = "LATE"

    producer.send(TOPIC, event)
    print(f"[{event_type:8s}] {event}")

    time.sleep(random.uniform(1, 2))