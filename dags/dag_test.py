from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def placeholder():
    print("DAG TEST — EN CONSTRUCCIÓN")
    print(f"Ejecutado: {datetime.now()}")

with DAG(
    dag_id="dag_test",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["test"]
) as dag:

    tarea = PythonOperator(
        task_id="placeholder",
        python_callable=placeholder
    )