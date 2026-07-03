"""
DAG: flybackDW_sp_ActivosRedeemCorp
Objetivo: Ejecutar sp_ActivosRedeemCorp() cada lunes a las 6am
          Recarga semanal de tblActivosRedeemCorp con validación
          de redeem_no = 1 para excluir clientes huérfanos
"""

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

with DAG(
    dag_id            = "flybackDW_sp_ActivosRedeemCorp",
    description       = "Recarga semanal de tblActivosRedeemCorp — activos con historia",
    schedule_interval = "0 6 * * 1",  # ← Cada lunes a las 6am
    start_date        = datetime(2026, 6, 26),
    catchup           = False,       # ← actualizado para evitar catchup
    tags              = ["flybackDW", "semanal", "activos", "mariadb"],
) as dag:
    sp_activos_redeem_corp = MySqlOperator(
        task_id       = "sp_ActivosRedeemCorp",
        mysql_conn_id = "MariaDB",
        sql           = "CALL flybackDW.sp_ActivosRedeemCorp();",
    )
    sp_activos_redeem_corp  # ← agregar esta línea