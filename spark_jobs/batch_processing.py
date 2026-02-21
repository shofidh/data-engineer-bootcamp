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
orders = spark.read.option("header", "true").csv("/opt/airflow/data/orders.csv")
order_items = spark.read.option("header", "true").csv("/opt/airflow/data/order_items.csv")

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

# ======================
# 4. Join Tabel
# ======================
joined_df = orders.join(order_items, on="order_id", how="inner")
print("Join Completed")

# ======================
# 5. Buat kolom turunan
# ======================
joined_df = joined_df.withColumn("gmv", col("quantity") * col("price"))

# ======================
# 6. Simpan hasil
# ======================
output_path = "/opt/airflow/data/output/fact_transactions"
joined_df.write.mode("overwrite").parquet(output_path)
print(f"Data saved to {output_path} in Parquet format")

# ======================
# 7. Validasi read
# ======================
parquet_df = spark.read.parquet(output_path)
parquet_df.show(5)

spark.stop()
print("Spark Session Stopped")