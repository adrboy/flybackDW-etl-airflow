"""
DAG: flybackDW_spInsertHistoricoCobranza
Objetivo: Ejecutar spInsertHistoricoCobranza(NULL) el primer día de cada mes
"""

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

with DAG(
    dag_id            = "flybackDW_spInsertHistoricoCobranza",
    description       = "Inserta el mes cerrado en tbl_historico_cobranza",
    schedule_interval = "0 6 1 * *",  # ← Primer día del mes a las 6am
    start_date        = datetime(2026, 7, 2),   # ← actualizado hoy — evita catchup
    catchup           = False,
    tags              = ["flybackDW", "historico", "mariadb"],
) as dag:
    insertar_historico = MySqlOperator(
        task_id       = "spInsertHistoricoCobranza",
        mysql_conn_id = "MariaDB",
        sql           = "CALL flybackDW.spInsertHistoricoCobranza(NULL);",
    )