"""
DAG: flybackDW_sp_FinalizarContratosVencidos
Objetivo: Ejecutar sp_FinalizarContratosVencidos() el 1 de febrero
          Red de seguridad anual — finaliza contratos cuyo último
          redeem fue en el año anterior o antes
"""

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

with DAG(
    dag_id            = "flybackDW_sp_FinalizarContratosVencidos",
    description       = "Finaliza contratos vencidos — red de seguridad anual",
    schedule_interval = "0 6 1 2 *",   # ← 1 febrero a las 6am
    start_date        = datetime(2027, 2, 1),
    catchup           = False,          # ← no ejecutar años anteriores
    tags              = ["flybackDW", "anual", "finalizados", "mariadb"],
) as dag:

    sp_finalizar = MySqlOperator(
        task_id       = "sp_FinalizarContratosVencidos",
        mysql_conn_id = "MariaDB",
        sql           = "CALL flybackDW.sp_FinalizarContratosVencidos();",
    )

    sp_finalizar