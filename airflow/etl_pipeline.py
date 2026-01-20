from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_script(script):
    subprocess.run(["python3", "/Users/vedanshnikum/Documents/data-warehouse-cntd/scripts/" + script], check=True)

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2026, 1, 19),
    schedule="@daily",
    catchup=False
) as dag:

    t1 = PythonOperator(task_id="bronze_ingest", python_callable=lambda: run_script("ingest_bronze.py"))
    t2 = PythonOperator(task_id="silver_transform", python_callable=lambda: run_script("transform_silver.py"))
    t3 = PythonOperator(task_id="gold_build", python_callable=lambda: run_script("build_gold.py"))

    t1 >> t2 >> t3
