import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_orders_from_postgres(**context):
    """
    Extract orders data from PostgreSQL.
    """
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        query = """
            SELECT *
            FROM raw_data.orders
            WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """

        records = hook.get_records(query)
        cursor = hook.get_cursor()
        columns = [desc[0] for desc in cursor.description]

        orders = [dict(zip(columns, row)) for row in records]

        context["task_instance"].xcom_push(
            key="orders_data",
            value=orders
        )

        logging.info("Extracted %s orders", len(orders))

    except Exception as e:
        logging.error("Failed extracting orders: %s", e)
        raise
