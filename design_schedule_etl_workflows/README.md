# PostgreSQL to MySQL ETL Pipeline with Apache Airflow

## 📌 Overview

Project ini merupakan implementasi **ETL (Extract, Transform, Load) pipeline** menggunakan **Apache Airflow** untuk memindahkan data dari **database operasional PostgreSQL** ke **data warehouse MySQL**.

Pipeline ini disusun untuk memenuhi kebutuhan bisnis **RetailCorp (E-commerce)** dalam menyediakan data yang up-to-date untuk keperluan **analytics dan reporting**, dengan sinkronisasi data otomatis setiap **6 jam**.

---

## 🎯 Business Case

**Company:** RetailCorp (E-commerce)

**Problem:**
Tim bisnis membutuhkan data transaksi terbaru dari sistem operasional (PostgreSQL) ke data warehouse (MySQL) agar laporan dapat dibuat secara near real-time.

**Solution:**
Membangun ETL pipeline berbasis Airflow yang:

* Menangani data yang terus berubah (incremental load)
* Melakukan transformasi sesuai kebutuhan bisnis
* Berjalan otomatis dan terjadwal
* Robust terhadap error

---

## 🏗️ Architecture

```
PostgreSQL (OLTP)
   |
   |  Extract (Airflow + PostgresHook)
   v
Apache Airflow DAG
   |
   |  Transform + Load (Airflow + MySqlHook)
   v
MySQL (Data Warehouse)
```

### Data Flow

* **Customers** → `dim_customers`
* **Products + Suppliers** → `dim_products`
* **Orders** → `fact_orders`

---

## 📂 Project Structure

```
airflow/
└── dags/
    ├── postgres_to_mysql_etl.py
    │
    └── src/
        ├── extract/
        │   ├── customers.py
        │   ├── products.py
        │   └── orders.py
        │
        └── transform_load/
            ├── customers.py
            ├── products.py
            └── orders.py
```

### Design Principles

* **Separation of Concerns**: DAG hanya untuk orchestration
* **Modular Code**: Extract dan Transform/Load dipisahkan
* **Production-ready**: Logging, error handling, retry mechanism

---

## ⏱️ DAG Configuration

| Parameter   | Value                                 |
| ----------- | ------------------------------------- |
| DAG Name    | `postgres_to_mysql_etl`               |
| Schedule    | Every 6 hours                         |
| Owner       | data-engineering-team                 |
| Retries     | 2                                     |
| Retry Delay | 5 minutes                             |
| Catchup     | Disabled                              |
| Tags        | etl, postgresql, mysql, data-pipeline |

---

## 🔄 ETL Details

### 1️⃣ Extract

* Source: PostgreSQL (`raw_data` schema)
* Incremental filter: `updated_at >= CURRENT_DATE - INTERVAL '1 day'`
* Output: List of dictionaries
* Data passed via **XCom**

**Extracted Entities:**

* Customers
* Products (joined with suppliers)
* Orders

---

### 2️⃣ Transform

#### Customers

* Phone number → formatted `(XXX) XXX-XXXX`
* State code → uppercase

#### Products

* Margin calculation: `((price - cost) / price) * 100`
* Category → Title Case

#### Orders

* Status → lowercase
* Total amount → validated (negative values set to 0)

---

### 3️⃣ Load

* Target: MySQL Data Warehouse
* Method: **UPSERT** (`INSERT ... ON DUPLICATE KEY UPDATE`)
* Tables:

  * `dim_customers`
  * `dim_products`
  * `fact_orders`

---

## 🔐 Connection Management

Database credentials are **NOT hardcoded**.

Connections are managed via **Airflow Connections UI**:

| Connection ID    | Type       |
| ---------------- | ---------- |
| postgres_default | PostgreSQL |
| mysql_default    | MySQL      |

---

## 🛠️ Error Handling & Logging

* Try-except blocks in setiap task
* Transaction rollback jika gagal
* Informative logging untuk:

  * Jumlah data diproses
  * Data anomaly (contoh: negative total_amount)

---

## 🚀 How to Run

1. Pastikan Docker & Docker Compose berjalan
2. Jalankan Airflow:

```bash
docker compose up -d
```

3. Akses Airflow UI:

```
http://localhost:8081
```

4. Aktifkan DAG `postgres_to_mysql_etl`

---