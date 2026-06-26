# ═══════════════════════════════════════════════════════
# DAG: dag_inicio_r_diario
# Objetivo: Actualizar columna inicio_r en customers
#           ejecutando SP customers.diario_update_inicio_r
# Carpeta: etl_flyback/
# Versión: 1.0 — 2026-06-25
# Migrado desde: Batch Navicat "Batch_Diario_Inicio_r"
# ═══════════════════════════════════════════════════════
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from datetime                 import datetime
from functools                import partial
import sys
sys.path.insert(0, '/opt/airflow/dags')
from common.audit_logger   import escribir_log_txt
from common.email_notifier import send_etl_notification
from common.db_connections import LOG_PATH
from common.sql_loader     import cargar_sql

DAG_ID = "dag_inicio_r_diario"

# ── Configuración de tarea ───────────────────────────────
TAREA = {
    "sp"           : "customers.diario_update_inicio_r",
    "vista_origen" : "customers.redeems",
    "tabla_destino": "customers.redeems",
    "sleep_seg"    : 0,
}

# ── Función reutilizable ─────────────────────────────────
def ejecutar_sp(tarea: dict):
    hook = MySqlHook(mysql_conn_id='MariaDB')
    try:
        hook.run(f"CALL {tarea['sp']}();")
        print(f"[{datetime.now()}] {tarea['sp']} — OK")

    except Exception as e:
        sql_error = cargar_sql(
            'sql/etl_flyback/insert_audit_log_error.sql'
           ,sp            = tarea['sp']
           ,vista_origen  = tarea['vista_origen']
           ,tabla_destino = tarea['tabla_destino']
           ,error         = str(e)[:500].replace("'", "")
        )
        hook.run(sql_error)
        raise


# ── Función log + email ──────────────────────────────────
def generar_log_y_notificar():
    mensaje = "\n".join([
        f"DAG: {DAG_ID} — INICIO",
        f"customers.diario_update_inicio_r — OK",
        f"DAG: {DAG_ID} — FIN ✅",
    ])
    log_path = escribir_log_txt(LOG_PATH, "inicio_r_diario", mensaje)
    send_etl_notification(
        dag_id   = DAG_ID,
        status   = "OK",
        log_path = log_path,
    )


# ── DAG ─────────────────────────────────────────────────
with DAG(
    dag_id            = DAG_ID,
    description       = "Actualiza inicio_r en customers.redeems via SP diario_update_inicio_r",
    schedule_interval = "0 8 * * 1-5",    # ← Lunes a Viernes 8:00am Cancún
    start_date        = datetime(2026, 6, 26),  # ← actualizado para evitar catchup
    catchup           = False,
    tags              = ["flybackDW", "redeems", "mariadb"],
) as dag:

    actualizar = PythonOperator(
        task_id         = "actualizar_inicio_r",
        python_callable = partial(ejecutar_sp, TAREA),
    )
    notificar = PythonOperator(
        task_id         = "generar_log_y_notificar",
        python_callable = generar_log_y_notificar,
    )

    # Secuencia: actualizar → log + email
    actualizar >> notificar
