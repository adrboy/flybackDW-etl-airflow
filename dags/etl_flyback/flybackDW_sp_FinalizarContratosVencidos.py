# ═══════════════════════════════════════════════════════
# DAG: flybackDW_sp_FinalizarContratosVencidos
# Objetivo: Red de seguridad anual — finaliza contratos
#           cuyo ultimo redeem fue en el año anterior o antes
#           Se ejecuta el 1 de febrero a las 6am
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

DAG_ID = "flybackDW_sp_FinalizarContratosVencidos"

# ── Configuracion ────────────────────────────────────────
TAREA = {
    "sp"           : "flybackDW.sp_FinalizarContratosVencidos",
    "vista_origen" : "flybackDW.tblActivosRedeemCorp",
    "tabla_destino": "flybackDW.tblActivosRedeemCorp",
}

# ── Ejecutar SP ──────────────────────────────────────────
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

# ── Log + email ──────────────────────────────────────────
def generar_log_y_notificar():
    mensaje = "\n".join([
        f"DAG: {DAG_ID} — INICIO",
        f"sp_FinalizarContratosVencidos() — OK",
        f"Red de seguridad anual ejecutada",
        f"DAG: {DAG_ID} — FIN ✅",
    ])
    log_path = escribir_log_txt(LOG_PATH, "sp_FinalizarContratosVencidos", mensaje)
    send_etl_notification(
        dag_id   = DAG_ID,
        status   = "OK",
        log_path = log_path,
    )

# ── DAG ─────────────────────────────────────────────────
with DAG(
    dag_id            = DAG_ID,
    description       = "Finaliza contratos vencidos — red de seguridad anual — 1 febrero 6am",
    schedule_interval = "0 6 1 2 *",    # ← 1 de febrero a las 6am
    start_date        = datetime(2027, 2, 1),
    catchup           = False,           # ← no ejecutar años anteriores
    tags              = ["flybackDW", "anual", "finalizados", "mariadb"],
) as dag:

    ejecutar = PythonOperator(
        task_id         = "sp_FinalizarContratosVencidos",
        python_callable = partial(ejecutar_sp, TAREA),
    )
    notificar = PythonOperator(
        task_id         = "generar_log_y_notificar",
        python_callable = generar_log_y_notificar,
    )

    ejecutar >> notificar
