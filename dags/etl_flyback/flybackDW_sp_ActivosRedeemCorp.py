# ═══════════════════════════════════════════════════════
# DAG: flybackDW_sp_ActivosRedeemCorp
# Objetivo: Recarga semanal de tblActivosRedeemCorp
#           ejecutando sp_ActivosRedeemCorp() cada lunes 6am
# Carpeta: etl_flyback/
# Version: 3.0 — 2026-09-14
# Cambios v3: schedule_interval semanal (lunes) -> diario 6am
#             SP migrado de TRUNCATE+INSERT a CDC idempotente
#             4 pasos: INSERT nuevos / UPDATE columnas / UPDATE Oportunidad / DELETE inactivos
#             Migrado tblJobsRegistros -> etl_audit_log
#             Agregadas columnas Create_At y Update_At a tblActivosRedeemCorp
# Cambios v2: MySqlOperator -> PythonOperator
#             Agrega email_notifier + audit_logger
#             Consistente con patron del resto de DAGs
# ═══════════════════════════════════════════════════════
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook  import MySqlHook
from datetime                  import datetime
from functools                 import partial
import sys
sys.path.insert(0, '/opt/airflow/dags')
from common.audit_logger   import escribir_log_txt
from common.email_notifier import send_etl_notification
from common.db_connections import LOG_PATH
from common.sql_loader     import cargar_sql

DAG_ID = "flybackDW_sp_ActivosRedeemCorp"

# ── Configuracion ────────────────────────────────────────
TAREA = {
    "sp"           : "flybackDW.sp_ActivosRedeemCorp",
    "vista_origen" : "customers.redeems + customers.fb_clients",
    "tabla_destino": "flybackDW.tblActivosRedeemCorp",
}

# ── Ejecutar SP ──────────────────────────────────────────
def ejecutar_sp(tarea: dict):
    hook = MySqlHook(mysql_conn_id='MariaDB')
    try:
        hook.run(f"CALL {tarea['sp']}();")
        print(f"[{datetime.now()}] {tarea['tabla_destino']} — OK")

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

# ── Log + email ──────────────────────────────────────────
def generar_log_y_notificar():
    mensaje = "\n".join([
        f"DAG: {DAG_ID} — INICIO",
        f"sp_ActivosRedeemCorp() — OK",
        f"Tabla: flybackDW.tblActivosRedeemCorp",
        f"DAG: {DAG_ID} — FIN ✅",
    ])
    log_path = escribir_log_txt(LOG_PATH, "sp_ActivosRedeemCorp", mensaje)
    send_etl_notification(
        dag_id   = DAG_ID,
        status   = "OK",
        log_path = log_path,
    )

# ── DAG ─────────────────────────────────────────────────
with DAG(
    dag_id            = DAG_ID,
    description       = "CDC diario de tblActivosRedeemCorp — 4 pasos idempotente (INSERT/UPDATE/UPDATE Oportunidad/DELETE)",
    schedule_interval = "0 6 * * *",     # <- cada dia 6:00am Cancun (antes solo lunes)
    start_date        = datetime(2026, 9, 14),
    catchup           = False,
    tags              = ["flybackDW", "diario", "activos", "cdc", "mariadb"],
) as dag:

    ejecutar = PythonOperator(
        task_id         = "sp_ActivosRedeemCorp",
        python_callable = partial(ejecutar_sp, TAREA),
    )
    notificar = PythonOperator(
        task_id         = "generar_log_y_notificar",
        python_callable = generar_log_y_notificar,
    )

    ejecutar >> notificar
