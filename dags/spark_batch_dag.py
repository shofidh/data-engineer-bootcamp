from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

with DAG(
    dag_id="spark_batch_processing",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/airflow/spark_jobs/batch_processing.py",
        conn_id="spark_default",
        verbose=True,
        conf={"spark.master": "spark://spark-master-sbp:7077"},
    )

    spark_job
    spark_job