# ═══════════════════════════════════════════════════════
# DAG: dag_limpieza_Mensual_tbInicioSolAutPag
# Objetivo: Limpieza mensual de registros huérfanos en
#           tblInicioSolicitados, tblInicioAutorizados
#           y tblInicioPagados en flybackDW
# Carpeta: etl_flyback/
# Versión: 1.0 — 2026-06-18
# Schedule: Día 2 de cada mes a las 1:00am Cancún
# ═══════════════════════════════════════════════════════
import pendulum
from airflow import DAG
from airflow.operators.python  import PythonOperator
from airflow.hooks.mysql_hook  import MySqlHook
from datetime                  import datetime
from pendulum import timezone
import sys
sys.path.insert(0, '/opt/airflow/dags')
from common.audit_logger   import escribir_log_txt
from common.email_notifier import send_etl_notification
from common.db_connections import LOG_PATH

DAG_ID = "dag_limpieza_Mensual_tbInicioSolAutPag"

# ── Función ETL ──────────────────────────────────────────
def ejecutar_limpieza():
    hook = MySqlHook(mysql_conn_id='MariaDB')
    hook.run("CALL flybackDW.sp_limpieza_Mensual_tbInicioSolAutPag();")
    print(f"[{datetime.now()}] sp_limpieza_Mensual_tbInicioSolAutPag — OK")

# ── Función log + email ───────────────────────────────────
def generar_log_y_notificar():
    mensaje = "\n".join([
        f"DAG: {DAG_ID} — INICIO",
        f"sp_limpieza_Mensual_tbInicioSolAutPag — OK",
        f"Tablas limpiadas:",
        f"  - flybackDW.tblInicioSolicitados",
        f"  - flybackDW.tblInicioAutorizados",
        f"  - flybackDW.tblInicioPagados",
        f"DAG: {DAG_ID} — FIN ✅",
    ])
    log_path = escribir_log_txt(LOG_PATH, "limpieza_Mensual_tbInicioSolAutPag", mensaje)
    send_etl_notification(
        dag_id   = DAG_ID,
        status   = "OK",
        log_path = log_path,
    )

# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id            = DAG_ID,
    description       = "Limpieza mensual de registros huérfanos en tablas Redeem DW",
    schedule_interval = "0 1 2 * *",  # ← Día 2 de cada mes a las 1:00am Cancún
    start_date        = pendulum.datetime(2026, 6, 18, tz="America/Cancun"),
    catchup           = False,
    timezone          = timezone("America/Cancun"),   # ← hora local Cancún
    tags              = ["flybackDW", "redeems", "mariadb", "limpieza"],
) as dag:

    limpiar = PythonOperator(
        task_id         = "ejecutar_sp_limpieza",
        python_callable = ejecutar_limpieza,
    )

    notificar = PythonOperator(
        task_id         = "generar_log_y_notificar",
        python_callable = generar_log_y_notificar,
    )

    # Secuencia: limpieza → log + email
    limpiar >> notificar
