from datetime import datetime
from airflow.decorators import dag
from airflow.operators.python import PythonOperator


def load_orders_from_csv(**kwargs):
    """загрузка заказов из CSV"""
    pass


def load_order_items_from_csv(**kwargs):
    """загрузка позиций заказов из CSV"""
    pass


def build_customer_summary(**kwargs):
    """построение customer_summary"""
    pass


@dag(schedule="@daily", start_date=datetime(2025, 6, 1), catchup=False)
def etl_orders_pipeline():
    load_orders_task = PythonOperator(
        task_id="load_orders",
        python_callable=load_orders_from_csv,
    )

    load_order_items_task = PythonOperator(
        task_id="load_order_items",
        python_callable=load_order_items_from_csv,
    )

    build_summary_task = PythonOperator(
        task_id="build_summary",
        python_callable=build_customer_summary,
    )

    load_orders_task >> load_order_items_task >> build_summary_task


dag = etl_orders_pipeline()