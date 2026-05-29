from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import polars as pl

def validar_polars():
    df = pl.DataFrame({"status": ["Ready"], "team": ["Gusa & Gemini"]})
    print("--- CERTIFICACIÓN DE POLARS ---")
    print(df)
    print("-------------------------------")

with DAG(
    dag_id='0_certificacion_entorno',
    start_date=datetime(2026, 4, 21),
    schedule_interval=None,
    catchup=False
) as dag:

    tarea_test = PythonOperator(
        task_id='validar_instalacion',
        python_callable=validar_polars
    )