import logging
import re
from airflow.providers.mysql.hooks.mysql import MySqlHook


def transform_and_load_customers(**context):
    """
    Transform customers data and load into MySQL.
    """
    customers = context["task_instance"].xcom_pull(
        task_ids="extract_customers",
        key="customers_data",
    )

    if not customers:
        logging.info("No customers to process")
        return

    mysql_hook = MySqlHook(mysql_conn_id="mysql_default")
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()

    try:
        for customer in customers:
            phone = re.sub(r"\D", "", customer.get("phone", ""))
            if len(phone) == 10:
                customer["phone"] = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"

            customer["state"] = customer.get("state", "").upper()

            sql = """
                INSERT INTO dim_customers (
                    id, name, email, phone, state, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name = VALUES(name),
                    email = VALUES(email),
                    phone = VALUES(phone),
                    state = VALUES(state),
                    updated_at = VALUES(updated_at)
            """

            cursor.execute(
                sql,
                (
                    customer["id"],
                    customer["name"],
                    customer["email"],
                    customer["phone"],
                    customer["state"],
                    customer["updated_at"],
                ),
            )

        conn.commit()
        logging.info("Customers loaded successfully")
    except Exception as e:
        conn.rollback()
        logging.error("Failed loading customers: %s", e)
        raise
    finally:
        cursor.close()
        conn.close()

