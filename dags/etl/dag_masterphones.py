from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="dag_masterphones",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 6 * * 1",
    catchup=False,
    tags=["bronze", "master"]
) as dag:
    trigger_phonefi = TriggerDagRunOperator(
        task_id="trigger_phonefi",
        trigger_dag_id="dag_phonefi_240",
        wait_for_completion=True
    )

    trigger_phonevc = TriggerDagRunOperator(
        task_id="trigger_phonevc",
        trigger_dag_id="dag_phonevc_240",
        wait_for_completion=True
    )

    trigger_phonefb = TriggerDagRunOperator(
        task_id="trigger_phonefb",
        trigger_dag_id="dag_phonefb_242",
        wait_for_completion=True
    )

    trigger_phonebb = TriggerDagRunOperator(
        task_id="trigger_phonebb",
        trigger_dag_id="dag_phonebb_242",
        wait_for_completion=True
    )

    trigger_phoneml = TriggerDagRunOperator(
        task_id="trigger_phoneml",
        trigger_dag_id="dag_phoneml_242",
        wait_for_completion=True
    )
    # Secuencia: 240 primero → después 242
    trigger_phonefi >> trigger_phonevc >> trigger_phonefb >> trigger_phonebb >> trigger_phoneml