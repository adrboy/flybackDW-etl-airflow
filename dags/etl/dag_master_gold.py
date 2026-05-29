from airflow import DAG
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime

with DAG(
    dag_id="dag_master_gold",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 7 * * 1",
    catchup=False,
    tags=["gold", "master"]
) as dag:
    
    # # Esperar que Bronze termine exitosamente
    # esperar_bronze = ExternalTaskSensor(
    #     task_id="esperar_bronze",
    #     external_dag_id="dag_masterphones",
    #     external_task_id=None,
    #     mode="reschedule",
    #     timeout=7200, # 2 horas
    #     poke_interval=60
    # )

    # SP 1 - Maestro
    sp_maestro = MsSqlOperator(
        task_id="sp_etl_maestro",
        mssql_conn_id="MSSQL244",
        sql="EXEC [dw_etl].[sp_etl_maestro]",
        autocommit=True
    )

    # SP 2 - Info Personal
    sp_phones = MsSqlOperator(
        task_id="sp_insert_phones",
        mssql_conn_id="MSSQL244",
        sql="EXEC [dw_etl].[sp_insert_phones_factPersonalInfo]",
        autocommit=True
    )

    # Secuencia
    #esperar_bronze >> sp_maestro >> sp_phones
    sp_maestro >> sp_phones