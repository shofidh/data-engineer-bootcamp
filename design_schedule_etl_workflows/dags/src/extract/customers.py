import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_customers_from_postgres(**context):
    """
    Extract customers data from PostgreSQL.
    """
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")
        query = """
            SELECT *
            FROM raw_data.customers
            WHERE updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """
        records = hook.get_records(query)
        columns = [col[0] for col in hook.get_cursor().description]

        customers = [dict(zip(columns, row)) for row in records]

        context["task_instance"].xcom_push(
            key="customers_data",
            value=customers,
        )

        logging.info("Extracted %s customers", len(customers))
    except Exception as e:
        logging.error("Failed extracting customers: %s", e)
        raise
