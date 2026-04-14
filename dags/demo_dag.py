from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def my_task():
    print("Hello Airflow Workflow 2026!")

with DAG(
    dag_id="demo_workflow",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",   # ✅ FIXED
    catchup=False,
) as dag:

    task1 = PythonOperator(
        task_id="print_message",
        python_callable=my_task
    )

    task1