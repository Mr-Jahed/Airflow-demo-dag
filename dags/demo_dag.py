from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import csv
import os

# -----------------------------
# 1. Extract Data (API Call)
# -----------------------------
def extract_data(**context):
    import requests

    url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(url)

    data = response.json()

    if not data:
        raise ValueError("No data received from API")

    context['ti'].xcom_push(key='raw_data', value=data)


# -----------------------------
# 2. Transform Data
# -----------------------------
def transform_data(**context):
    raw_data = context['ti'].xcom_pull(task_ids='extract_data', key='raw_data')

    if raw_data is None:
        raise ValueError("No data found in XCom from extract_data")

    transformed = []
    for record in raw_data[:10]:
        transformed.append({
            "id": record["id"],
            "title": record["title"].upper(),
        })

    context['ti'].xcom_push(key='clean_data', value=transformed)


# -----------------------------
# 3. Save to CSV
# -----------------------------
def save_to_csv(**context):
    clean_data = context['ti'].xcom_pull(
        task_ids='transform_data',   # ✅ IMPORTANT FIX
        key='clean_data'
    )

    if clean_data is None:
        raise ValueError("No transformed data found in XCom")

    file_path = "/usr/local/airflow/dags/output.csv"

    import csv
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=["id", "title"])
        writer.writeheader()
        writer.writerows(clean_data)

    print(f"File saved at {file_path}")


# -----------------------------
# 4. Load to DB (Simulated)
# -----------------------------
def load_to_db():
    print("Data loaded into database successfully (simulated)")


# -----------------------------
# DAG Definition
# -----------------------------
with DAG(
    dag_id="etl_pipeline_demo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl", "api", "demo"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    save_task = PythonOperator(
        task_id="save_to_csv",
        python_callable=save_to_csv
    )

    load_task = PythonOperator(
        task_id="load_to_db",
        python_callable=load_to_db
    )

    # Task Flow (IMPORTANT)
    extract_task >> transform_task >> save_task >> load_task