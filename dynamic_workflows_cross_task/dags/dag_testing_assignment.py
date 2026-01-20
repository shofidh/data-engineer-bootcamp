"""
DAG: data_validation_dag
Purpose: Demonstrate unit testing & integration testing in Airflow
Owner: data-engineering-team
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments
default_args = {
    "owner": "data-engineering-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Business Logic Functions (TESTABLE)
def extract_data(**context):
    """
    Extract sample data.
    Returns a list of dictionaries.
    """
    try:
        data = [
            {"name": "apple", "price": 10},
            {"name": "banana", "price": 5},
        ]
        logging.info("Extracted data: %s", data)
        context["ti"].xcom_push(key="raw_data", value=data)
        return data
    except Exception as e:
        logging.error("Error in extract_data: %s", e)
        raise


def transform_data(data):
    """
    Transform data:
    - Uppercase product name
    - Add derived field 'price_with_tax'
    """
    try:
        transformed = []
        for item in data:
            transformed.append(
                {
                    "name": item["name"].upper(),
                    "price": item["price"],
                    "price_with_tax": item["price"] * 1.1,
                }
            )
        logging.info("Transformed data: %s", transformed)
        return transformed
    except Exception as e:
        logging.error("Error in transform_data: %s", e)
        raise


def transform_task(**context):
    """
    Pull data from extract_task using XCom,
    then apply transform_data().
    """
    ti = context["ti"]
    raw_data = ti.xcom_pull(task_ids="extract_task", key="raw_data")

    if not raw_data:
        raise ValueError("No data received from extract_task")

    result = transform_data(raw_data)
    ti.xcom_push(key="transformed_data", value=result)
    return result


def load_data(**context):
    """
    Simulate loading data.
    """
    try:
        ti = context["ti"]
        data = ti.xcom_pull(task_ids="transform_task", key="transformed_data")
        if not data:
            raise ValueError("No transformed data found")

        logging.info("Loading data to target system: %s", data)
    except Exception as e:
        logging.error("Error in load_data: %s", e)
        raise


# DAG Definition
with DAG(
    dag_id="data_validation_dag",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["testing", "validation", "dag-testing"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
        provide_context=True,
    )

    transform_task_op = PythonOperator(
        task_id="transform_task",
        python_callable=transform_task,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
        provide_context=True,
    )

    extract_task >> transform_task_op >> load_task
