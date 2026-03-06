from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, to_timestamp, struct,
    when, window, count, current_timestamp, lit,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType
)


# Spark Session
# Kafka JAR passed via --packages in spark-submit:
#   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1

spark = (
    SparkSession.builder
    .appName("TransactionStreaming")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Suppress noisy state store warnings yang spam di console
log4j = spark.sparkContext._jvm.org.apache.log4j
log4j.Logger.getLogger("org.apache.spark.sql.execution.streaming").setLevel(log4j.Level.ERROR)
log4j.Logger.getLogger("org.apache.spark.sql.kafka010").setLevel(log4j.Level.ERROR)
log4j.Logger.getLogger("org.apache.spark.storage.HDFSBackedStateStoreProvider").setLevel(log4j.Level.OFF)
log4j.Logger.getLogger("org.apache.spark.sql.execution.streaming.state").setLevel(log4j.Level.OFF)
log4j.Logger.getLogger("org.apache.kafka.clients").setLevel(log4j.Level.ERROR)


# Schema

schema = StructType([
    StructField("user_id",   StringType(), True),
    StructField("amount",    LongType(),   True),
    StructField("timestamp", StringType(), True),
    StructField("source",    StringType(), True),
])

VALID_SOURCES = ["mobile", "web", "pos"]
KAFKA_BROKER  = "kafka:29092"


# 1. Read raw stream from Kafka

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .load()
)


# 2. Deserialise JSON → typed columns
#    Simpan kafka_timestamp untuk deteksi late event

parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
    .select(from_json(col("json_str"), schema).alias("d"), col("kafka_ts"))
    .select("d.*", "kafka_ts")
)


# 3. Parse event_time

with_time = parsed_df.withColumn(
    "event_time", to_timestamp(col("timestamp"))
)


# 4. Deteksi late event SEBELUM watermark
#    Late = selisih antara kafka processing time dan event_time > 3 menit

with_late_flag = with_time.withColumn(
    "is_late",
    when(
        col("event_time").isNotNull() &
        (col("kafka_ts").cast("long") - col("event_time").cast("long") > 180),
        True
    ).otherwise(False)
)


# 5. Apply watermark

with_watermark = with_late_flag.withWatermark("event_time", "3 minutes")


# 6. Duplicate detection

deduped = with_watermark.dropDuplicates(["user_id", "timestamp"])


# 7. Five mandatory validations + late event

validated = (
    deduped
    .withColumn(
        "error_reason",
        when(col("user_id").isNull(),
             lit("missing_user_id"))
        .when(col("amount").isNull(),
             lit("missing_amount"))
        .when(col("timestamp").isNull() | col("event_time").isNull(),
             lit("invalid_timestamp"))
        .when((col("amount") < 1) | (col("amount") > 10_000_000),
             lit("amount_out_of_range"))
        .when(~col("source").isin(VALID_SOURCES),
             lit("unknown_source"))
        .when(col("is_late"),
             lit("late_event"))
        .otherwise(lit(None).cast(StringType()))
    )
    .withColumn(
        "is_valid",
        col("error_reason").isNull()
    )
    .drop("is_late", "kafka_ts")
)


# 8. Split valid / invalid

valid_df   = validated.filter(col("is_valid") == True)
invalid_df = validated.filter(col("is_valid") == False)


# 9. Sink: valid → transactions_valid

valid_query = (
    valid_df
    .select(to_json(struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", "transactions_valid")
    .option("checkpointLocation", "/tmp/checkpoints/valid")
    .outputMode("append")
    .start()
)


# 10. Sink: invalid → transactions_dlq

dlq_query = (
    invalid_df
    .select(to_json(struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", "transactions_dlq")
    .option("checkpointLocation", "/tmp/checkpoints/dlq")
    .outputMode("append")
    .start()
)


# 11. Tumbling window monitoring — foreachBatch + update mode
#
#     FIX: running_total sekarang menunjukkan jumlah transaksi PER WINDOW
#     bukan akumulasi global. Setiap baris = laporan 1 jendela waktu 1 menit.
#
#     Contoh output yang benar:
#     window 21:39-21:40 → running_total: 28   ← transaksi di menit itu
#     window 21:40-21:41 → running_total: 31   ← transaksi di menit itu
#     window 21:41-21:42 → running_total: 29   ← transaksi di menit itu

def process_window_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    # Tambah timestamp kapan Spark output batch ini
    # running_total sudah berisi count per window dari groupBy di atas
    # tidak perlu accumulator — langsung pakai nilai dari aggregation
    output_df = (
        batch_df
        .withColumn("timestamp", current_timestamp())
        .select(
            col("timestamp"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("running_total"),    # jumlah transaksi valid di window ini
        )
        .orderBy("window_start")     # urutkan dari window paling lama
    )

    total_batch = batch_df.agg({"running_total": "sum"}).collect()[0][0] or 0

    print(f"\n{'='*65}")
    print(f"  Batch ID        : {batch_id}")
    print(f"  Windows aktif   : {batch_df.count()}")
    print(f"  Total transaksi di batch ini : {total_batch}")
    print(f"{'='*65}")
    output_df.show(truncate=False)


window_agg = (
    valid_df
    .groupBy(window(col("event_time"), "1 minute"))
    .agg(count("*").alias("running_total"))
)

console_query = (
    window_agg
    .writeStream
    .outputMode("update")
    .foreachBatch(process_window_batch)
    .trigger(processingTime="15 seconds")
    .option("checkpointLocation", "/tmp/checkpoints/console")
    .start()
)

print("[INFO] All streaming queries started.")
print(f"[INFO] Kafka broker  : {KAFKA_BROKER}")
print("[INFO] Input topic   : transactions")
print("[INFO] Valid sink    : transactions_valid")
print("[INFO] Invalid sink  : transactions_dlq")

spark.streams.awaitAnyTermination()