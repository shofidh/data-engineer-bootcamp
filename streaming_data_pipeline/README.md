# Kafka Streaming Transaction Validation

Mini streaming pipeline menggunakan **Apache Kafka** dan **PySpark Structured Streaming** untuk mensimulasikan dan memvalidasi transaksi secara real-time.

---

## Architecture
![Architecture](https://github.com/shofidh/data-engineer-bootcamp/blob/main/streaming_data_pipeline/screenshots/architecture.png)


---

## Features

- Event producer setiap 1–2 detik
- Simulasi 5 jenis invalid event (amount negatif, amount terlalu besar, timestamp tidak valid, source tidak dikenal, user_id kosong)
- Simulasi 3 late events (5, 7, 10 menit terlambat)
- Simulasi duplicate event
- 5 validasi data wajib
- Watermark handling (3 menit)
- Late event detection → otomatis ke DLQ dengan label `late_event`
- Tumbling window aggregation (1 menit)

---

## Tech Stack

| Tool | Versi | Keterangan |
|---|---|---|
| Apache Kafka | 7.3.0 (Confluent) | Message broker |
| PySpark Structured Streaming | 3.5.1 | Stream processing engine |
| Python | 3.x | Producer script |
| Docker | - | Container orchestration |

---

## Project Structure

```
streaming_data_pipeline/
├── producer/
│   └── producer.py              # Kafka event producer
├── streaming/
│   └── spark_streaming_job.py   # PySpark streaming 
├── docker-compose.yml
└── README.md
```

---

## Prerequisites

- Docker Desktop terinstall dan running
- Python 3.x terinstall di host machine
- Library `kafka-python` terinstall:

```bash
pip install kafka-python
```

---

## How to Run

### Step 1 — Start semua services dengan Docker

```bash
docker compose up -d
```

Tunggu semua container berstatus `healthy` (~30 detik):

```bash
docker ps
```

Output yang diharapkan:
```
CONTAINER ID   NAME             STATUS
xxxx           kafka-sdp        Up (healthy)
xxxx           zookeeper-sdp    Up (healthy)
xxxx           spark-sdp        Up
xxxx           kafka-ui-sdp     Up
```

---

### Step 2 — Buat Kafka topics

Buat 3 topics yang dibutuhkan pipeline:

```bash
# Topic utama (input dari producer)
docker exec -it kafka-sdp kafka-topics --create --topic transactions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Topic untuk data valid
docker exec -it kafka-sdp kafka-topics --create --topic transactions_valid --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# Topic untuk data invalid / DLQ
docker exec -it kafka-sdp kafka-topics --create --topic transactions_dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

Verifikasi topics sudah terbuat:

```bash
docker exec -it kafka-sdp kafka-topics --bootstrap-server localhost:9092 --list
```

Output:
```
transactions
transactions_valid
transactions_dlq
```

---

### Step 3 — Jalankan Spark Streaming Job

Buka **terminal pertama** dan jalankan streaming job di dalam Docker container:

```bash
docker exec -it spark-sdp /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /app/streaming/spark_streaming_job.py
```

> **Catatan:** Pertama kali menjalankan, Spark akan download Kafka JAR dari Maven Central (~1-2 menit). Download hanya terjadi sekali, selanjutnya sudah ter-cache.

Tunggu sampai muncul:
```
[INFO] All streaming queries started.
[INFO] Kafka broker  : kafka:29092
[INFO] Input topic   : transactions
[INFO] Valid sink    : transactions_valid
[INFO] Invalid sink  : transactions_dlq
```

---

### Step 4 — Jalankan Producer

Buka **terminal kedua** dan jalankan producer dari host machine:

```bash
python producer/producer.py
```

> **Kenapa producer pakai `python` bukan `spark-submit`?**
> Producer adalah script Python biasa yang hanya butuh library `kafka-python`. Tidak ada Spark di dalamnya, sehingga cukup dijalankan dengan Python langsung dari host machine menggunakan `localhost:9092`.
>
> Berbeda dengan streaming job yang membutuhkan JVM + Spark engine + Kafka connector JAR, sehingga harus dijalankan via `spark-submit` di dalam Docker container menggunakan `kafka:29092` (Docker internal network).

Output producer:
```
[Producer] Started → topic: transactions
[DUPLICATE] {'user_id': 'U7771', 'amount': 200000, 'timestamp': '2025-03-06T10:00:00Z', 'source': 'mobile'}
[DUPLICATE] {'user_id': 'U7771', 'amount': 200000, 'timestamp': '2025-03-06T10:00:00Z', 'source': 'mobile'}
[VALID   ] {'user_id': 'U3421', 'amount': 45000, 'timestamp': '2026-03-06T21:22:01Z', 'source': 'web'}
[INVALID ] {'user_id': 'U9991', 'amount': -500, 'timestamp': '2026-03-06T21:22:02Z', 'source': 'mobile'}
[LATE    ] {'user_id': 'U8881', 'amount': 50000, 'timestamp': '2026-03-06T21:17:01Z', 'source': 'mobile'}
```

![producer](https://github.com/shofidh/data-engineer-bootcamp/blob/main/streaming_data_pipeline/screenshots/producer.png)

---

## Validation Rules

Pipeline menerapkan 5 validasi wajib pada setiap event:

| # | Validasi | Kondisi Invalid | error_reason |
|---|---|---|---|
| 1 | Mandatory field check | `user_id` adalah null | `missing_user_id` |
| 2 | Mandatory field check | `amount` adalah null | `missing_amount` |
| 3 | Type validation | `timestamp` tidak bisa di-parse | `invalid_timestamp` |
| 4 | Range validation | `amount` < 1 atau > 10.000.000 | `amount_out_of_range` |
| 5 | Source validation | `source` bukan mobile/web/pos | `unknown_source` |
| + | Late event | Event terlambat > 3 menit | `late_event` |
| + | Duplicate | `user_id` + `timestamp` sama | di-drop via `dropDuplicates` |

---

## Watermark & Late Event Handling

```
Watermark = 3 menit

Event timestamp: 21:17:00
Spark processing time: 21:22:00
Selisih: 5 menit > 3 menit (watermark)
→ error_reason: "late_event"
→ Dikirim ke transactions_dlq ❌

Event timestamp: 21:21:00
Spark processing time: 21:22:00
Selisih: 1 menit < 3 menit (watermark)
→ Dianggap on-time → validasi normal ✅
```

---

## Tumbling Window Monitoring

Window size **1 menit** berbasis **event_time** (bukan processing time).

```
Timeline event_time:
─────────────────────────────────────────►
21:21:00    21:22:00    21:23:00    21:24:00
[==window==][==window==][==window==]
  1 menit     1 menit     1 menit
```

Console output (muncul tiap 15 detik):

```
=================================================================
  Batch ID : 8
  Kumulatif valid transactions : 146
=================================================================
+--------------------------+-------------------+-------------------+-------------+
|timestamp                 |window_start       |window_end         |running_total|
+--------------------------+-------------------+-------------------+-------------+
|2026-03-06 21:22:40.884  |2026-03-06 21:22:00|2026-03-06 21:23:00|146          |
+--------------------------+-------------------+-------------------+-------------+
```

| Kolom | Deskripsi |
|---|---|
| `timestamp` | Waktu Spark mencetak output batch ini |
| `window_start` | Awal periode 1 menit |
| `window_end` | Akhir periode 1 menit |
| `running_total` | Total kumulatif transaksi valid sejak streaming start |

![streaming output](https://github.com/shofidh/data-engineer-bootcamp/blob/main/streaming_data_pipeline/screenshots/streaming_output.png)

---

## Kafka UI

Akses Kafka UI di browser: **http://localhost:8080**

Dari sini kamu bisa monitor:
- Messages yang masuk ke topic `transactions`
- Messages valid di topic `transactions_valid`
- Messages invalid/late di topic `transactions_dlq`

![kafka ui](https://github.com/shofidh/data-engineer-bootcamp/blob/main/streaming_data_pipeline/screenshots/kafka_ui.png)

---

## Troubleshooting

**Error: `Failed to find data source: kafka`**
→ Pastikan menjalankan dengan `--packages`, bukan langsung `python3`

**Error: `Connection refused kafka:29092`**
→ Pastikan semua container ada di network yang sama di `docker-compose.yml`

**Console output kosong / batch selalu empty**
→ Pastikan producer sudah running di terminal terpisah

**Error: `FileNotFoundException /home/spark/.ivy2/cache`**
→ Jalankan dulu: `docker exec -u root -it spark-sdp mkdir -p /home/spark/.ivy2/cache`