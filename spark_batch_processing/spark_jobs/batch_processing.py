# sparks_batch_processing/spark_jobs/batch_processing.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# ======================
# 1. Inisialisasi Spark
# ======================
spark = SparkSession.builder \
    .appName("BatchProcessingAssignment") \
    .getOrCreate()

print("Spark Session Started...")

# ======================
# 2. Load dataset
# ======================
orders = spark.read.option("header", "true").csv("/opt/data/orders.csv")
order_items = spark.read.option("header", "true").csv("/opt/data/order_items.csv")

print("Data Loaded Successfully")

# ======================
# 3. Transformasi Data
# ======================

# 3a. Pembersihan Data
# Drop kolom tidak penting jika ada
if "total_amount" in orders.columns:
    orders = orders.drop("total_amount")

# Filter order_items quantity valid
order_items = order_items.filter(col("quantity") > 0)

# Fill NA / null
orders = orders.fillna({"user_id": -1})

# 3b. Standarisasi Data
orders = orders.withColumn("order_id", col("order_id").cast("int"))
orders = orders.withColumn("user_id", col("user_id").cast("int"))

order_items = order_items.withColumn("order_id", col("order_id").cast("int"))
order_items = order_items.withColumn("product_id", col("product_id").cast("int"))
order_items = order_items.withColumn("quantity", col("quantity").cast("int"))
order_items = order_items.withColumn("price", col("price").cast("double"))

# Standarisasi tanggal
orders = orders.withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

# Rename kolom
orders = orders.withColumnRenamed("user_id", "customer_id")

import sys
# ======================
# 4. Join Tabel
# ======================
print(f"DEBUG: orders count before join = {orders.count()}")
print(f"DEBUG: order_items count before join = {order_items.count()}")
joined_df = orders.join(order_items, on="order_id", how="inner")
print(f"DEBUG: joined_df count after join = {joined_df.count()}")
print("Join Completed")

# ======================
# 5. Buat kolom turunan
# ======================
joined_df = joined_df.withColumn("gmv", col("quantity") * col("price"))
print(f"DEBUG: final joined_df count = {joined_df.count()}")


# ======================
# 6. Simpan hasil
# ======================
import os

output_path = "/opt/data/output/fact_transactions.parquet"

# Explicitly create target directory
os.makedirs(os.path.dirname(output_path), exist_ok=True)

# Convert to Pandas and write to bypass Hadoop FileSystem Mkdirs limitations on Windows Docker mounts
print("Converting to Pandas for native single-file Parquet write...")
joined_df.toPandas().to_parquet(output_path, engine="pyarrow")

print(f"Data saved to {output_path} in Parquet format")

# ======================
# 7. Validasi read
# ======================
parquet_df = spark.read.parquet(output_path)
parquet_df.show(5)

spark.stop()
print("Spark Session Stopped")