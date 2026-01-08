import logging
from airflow.providers.mysql.hooks.mysql import MySqlHook


def transform_and_load_orders(**context):
    """
    Transform orders data and load into MySQL.
    """
    orders = context["task_instance"].xcom_pull(
        task_ids="extract_orders",
        key="orders_data"
    )

    if not orders:
        logging.info("No orders to process")
        return

    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    try:
        for order in orders:
            status = (order.get("status") or "").lower()

            total_amount = order.get("total_amount", 0) or 0
            if total_amount < 0:
                logging.warning(
                    "Negative total_amount for order_id %s, set to 0",
                    order.get("id")
                )
                total_amount = 0

            sql = """
                INSERT INTO fact_orders (
                    id,
                    customer_id,
                    order_date,
                    status,
                    total_amount,
                    updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    customer_id = VALUES(customer_id),
                    order_date = VALUES(order_date),
                    status = VALUES(status),
                    total_amount = VALUES(total_amount),
                    updated_at = VALUES(updated_at)
            """

            cursor.execute(
                sql,
                (
                    order["id"],
                    order["customer_id"],
                    order["order_date"],
                    status,
                    total_amount,
                    order["updated_at"],
                )
            )

        conn.commit()
        logging.info("Orders loaded successfully")

    except Exception as e:
        conn.rollback()
        logging.error("Failed loading orders: %s", e)
        raise
    finally:
        cursor.close()
        conn.close()
