from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from src.extract.customers import extract_customers_from_postgres
from src.extract.products import extract_products_from_postgres
from src.extract.orders import extract_orders_from_postgres

from src.transform_load.customers import transform_and_load_customers
from src.transform_load.products import transform_and_load_products
from src.transform_load.orders import transform_and_load_orders


default_args = {
    "owner": "data-engineering-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="postgres_to_mysql_etl",
    default_args=default_args,
    schedule_interval=timedelta(hours=6),
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "postgresql", "mysql", "data-pipeline"],
)

extract_customers = PythonOperator(
    task_id="extract_customers",
    python_callable=extract_customers_from_postgres,
    dag=dag,
)

extract_products = PythonOperator(
    task_id="extract_products",
    python_callable=extract_products_from_postgres,
    dag=dag,
)

extract_orders = PythonOperator(
    task_id="extract_orders",
    python_callable=extract_orders_from_postgres,
    dag=dag,
)

load_customers = PythonOperator(
    task_id="transform_load_customers",
    python_callable=transform_and_load_customers,
    dag=dag,
)

load_products = PythonOperator(
    task_id="transform_load_products",
    python_callable=transform_and_load_products,
    dag=dag,
)

load_orders = PythonOperator(
    task_id="transform_load_orders",
    python_callable=transform_and_load_orders,
    dag=dag,
)

extract_customers >> load_customers
extract_products >> load_products
extract_orders >> load_orders