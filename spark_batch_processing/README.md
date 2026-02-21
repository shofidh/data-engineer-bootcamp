<<<<<<< HEAD:spark_batch_processing/README.md
# data-engineer-toolkit

## airflow
`docker compose --profile airflow up -d`

## grafana
`docker compose --profile grafana up -d`

## postgres
`docker compose --profile postgres up -d`
or `docker compose --profile db up -d` (mysql + postgres)

## mysql
`docker compose --profile mysql up -d`
or `docker compose --profile db up -d` (mysql + postgres)

## hive
`docker compose --profile hive up -d`

## kafka
`docker compose --profile kafka up -d`

## spark
`docker compose --profile spark up -d`
=======
# PySpark Batch Processing Pipeline

This project implements a complete Spark batch processing pipeline that reads CSV data, performs data cleaning, standardizes schemas, joins datasets, generates derived metrics (GMV), and outputs the final structured data as a Parquet file. The entire process is orchestrated via Apache Airflow.

## Project Architecture

- **Airflow**: Orchestrates the pipeline execution and triggers the Spark job via the `SparkSubmitOperator`. Runs in standalone mode.
- **Spark Cluster**: Composed of a Spark Master and a Spark Worker (using Bitnami images).
- **Storage Volumes**: Uses Docker volumes mapping `./data` to `/opt/data` and `./spark_jobs` to `/opt/spark_jobs` to safely share data and code between Airflow and the Spark cluster.

## How to Run the Pipeline

1. **Start the Infrastructure**
   Launch both the Spark cluster and Airflow using Docker Compose profiles:
   ```bash
   docker compose --profile spark --profile airflow up -d --build
   ```

2. **Access Airflow UI**
   - Wait for the Airflow webserver and database to initialize.
   - Navigate to `http://localhost:8080/` in your browser.
   - Log in with:
     - **Username**: `admin`
     - **Password**: `admin123`

3. **Execute the DAG**
   - Locate the `spark_batch_processing` DAG.
   - Unpause the DAG using the toggle switch.
   - Click the "Trigger DAG" (play) button to start the run.
   - The task `run_spark_job` will submit `batch_processing.py` to the Spark cluster.

4. **Verify Output**
   - Once the DAG execution completes successfully, check the `data/output/` directory.
   - You will find `fact_transactions.parquet` generated successfully.

## Technical Notes
- The PySpark job leverages `toPandas().to_parquet()` logic for its final sink to explicitly bypass Hadoop `LocalFileSystem` permission bugs ("Mkdirs failed") that natively occur on Windows WSL2 Docker volume bind-mounts when outputting Parquet directories.
>>>>>>> 017667f (Complete PySpark Batch Processing pipeline implementation):README.md
