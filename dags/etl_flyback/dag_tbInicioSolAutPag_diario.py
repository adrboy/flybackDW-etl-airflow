# ═══════════════════════════════════════════════════════
# DAG: dag_tbInicioSolAutPag_diario
# Objetivo: Actualizar tblInicioSolicitados, tblInicioAutorizados
#           y tblInicioPagados en flybackDW
# Carpeta: etl_flyback/
# Versión: 1.1 — 2026-06-15
# ═══════════════════════════════════════════════════════

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime
from common.email_notifier import send_etl_notification
from airflow.operators.python import PythonOperator

with DAG(
    dag_id            = "dag_tbInicioSolAutPag_diario",
    description       = "Actualiza tblInicioSolicitados, tblInicioAutorizados y tblInicioPagados en flybackDW",
    schedule_interval = "0 6 * * *",  # ← Diario a las 6am
    start_date        = datetime(2026, 6, 11),
    catchup           = False,        # ← No reprocesa días anteriores
    tags              = ["flybackDW", "redeems", "mariadb"],
) as dag:

    actualizar_sol = MySqlOperator(
        task_id       = "actualizar_tblInicioSolicitados",
        mysql_conn_id = "MariaDB",
        sql           = "CALL flybackDW.update_flybackDW_tblInicioSolicitados_VI_hour();",
    )

    actualizar_aut = MySqlOperator(
        task_id       = "actualizar_tblInicioAutorizados",
        mysql_conn_id = "MariaDB",
        sql           = "CALL flybackDW.update_flybackDW_tblInicioAutorizados_VI_hour();",
    )

    actualizar_pag = MySqlOperator(
        task_id       = "actualizar_tblInicioPagados",
        mysql_conn_id = "MariaDB",
        sql           = "CALL flybackDW.update_flybackDW_tblInicioPagados_VI_hour();",
    )

    # Notificación por email al terminar
    notificar = PythonOperator(
        task_id         = "notificar_email",
        python_callable = send_etl_notification,
        op_kwargs       = {
            "dag_id"  : "dag_tbInicioSolAutPag_diario",
            "status"  : "OK",
            "log_path": None,
        },
    )

    # Secuencia completa: sol → aut → pag → email
    actualizar_sol >> actualizar_aut >> actualizar_pag >> notificar
