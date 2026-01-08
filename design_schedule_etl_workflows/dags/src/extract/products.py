import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract_products_from_postgres(**context):
    """
    Extract products data from PostgreSQL with supplier information.
    """
    try:
        hook = PostgresHook(postgres_conn_id="postgres_default")

        query = """
            SELECT
                p.id,
                p.product_name,
                p.category,
                p.price,
                p.cost,
                p.updated_at,
                s.supplier_name
            FROM raw_data.products p
            JOIN raw_data.suppliers s
                ON p.supplier_id = s.id
            WHERE p.updated_at >= CURRENT_DATE - INTERVAL '1 day'
        """

        records = hook.get_records(query)
        cursor = hook.get_cursor()
        columns = [desc[0] for desc in cursor.description]

        products = [dict(zip(columns, row)) for row in records]

        context["task_instance"].xcom_push(
            key="products_data",
            value=products
        )

        logging.info("Extracted %s products", len(products))

    except Exception as e:
        logging.error("Failed extracting products: %s", e)
        raise
