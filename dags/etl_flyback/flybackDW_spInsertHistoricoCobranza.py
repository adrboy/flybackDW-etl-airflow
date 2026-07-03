# ═══════════════════════════════════════════════════════
# DAG: flybackDW_spInsertHistoricoCobranza
# Objetivo: Insertar mes cerrado en tbl_historico_cobranza
#           ejecutando spInsertHistoricoCobranza(NULL)
#           el primer dia de cada mes a las 6am
# Carpeta: etl_flyback/
# Version: 2.0 — 2026-07-03
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

DAG_ID = "flybackDW_spInsertHistoricoCobranza"

# ── Configuracion ────────────────────────────────────────
TAREA = {
    "sp"           : "flybackDW.spInsertHistoricoCobranza",
    "vista_origen" : "customers.cardcollec + customers.pk_ticket",
    "tabla_destino": "flybackDW.tbl_historico_cobranza",
}

# ── Ejecutar SP ──────────────────────────────────────────
def ejecutar_sp(tarea: dict):
    hook = MySqlHook(mysql_conn_id='MariaDB')
    try:
        hook.run(f"CALL {tarea['sp']}(NULL);")
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
        f"spInsertHistoricoCobranza(NULL) — OK",
        f"Tabla: flybackDW.tbl_historico_cobranza",
        f"DAG: {DAG_ID} — FIN ✅",
    ])
    log_path = escribir_log_txt(LOG_PATH, "spInsertHistoricoCobranza", mensaje)
    send_etl_notification(
        dag_id   = DAG_ID,
        status   = "OK",
        log_path = log_path,
    )

# ── DAG ─────────────────────────────────────────────────
with DAG(
    dag_id            = DAG_ID,
    description       = "Inserta mes cerrado en tbl_historico_cobranza — dia 1 de cada mes 6am",
    schedule_interval = "0 6 1 * *",    # ← dia 1 de cada mes a las 6am Cancun
    start_date        = datetime(2026, 7, 2),
    catchup           = False,
    tags              = ["flybackDW", "historico", "mariadb"],
) as dag:

    insertar = PythonOperator(
        task_id         = "spInsertHistoricoCobranza",
        python_callable = partial(ejecutar_sp, TAREA),
    )
    notificar = PythonOperator(
        task_id         = "generar_log_y_notificar",
        python_callable = generar_log_y_notificar,
    )

    # Secuencia: insertar -> log + email
    insertar >> notificar
