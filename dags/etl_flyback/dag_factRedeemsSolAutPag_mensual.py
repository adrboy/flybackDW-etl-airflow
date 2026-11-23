# ═══════════════════════════════════════════════════════
# DAG: dag_factRedeemsSolAutPag_mensual
# Objetivo: Carga mensual de factredeemssolicitados,
#           factredeemsautorizados y factredeemspagados
#           en flybackDW (bloque 1 mes anterior + bloque 2
#           limpieza, revividos y actualización de fechas)
# Carpeta: etl_flyback/
# Versión: 1.0 — 2026-11-22
# Schedule: Día 2 de cada mes a las 2:00am Cancún
#           (después de dag_limpieza_Mensual que corre a la 1:00am)
# ═══════════════════════════════════════════════════════
import pendulum
from airflow                  import DAG
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

DAG_ID = "dag_factRedeemsSolAutPag_mensual"

# ── Configuración de tareas — única fuente de verdad ────
# Para agregar un SP nuevo: solo agrega un dict a esta lista
TAREAS = [
    {
        "sp"           : "CFB.spSolicitadosDW",
        "vista_origen" : "customers.redeems",
        "tabla_destino": "flybackDW.factredeemssolicitados",
        "sleep_seg"    : 0,
    },
    {
        "sp"           : "CFB.spAutorizadosDW",
        "vista_origen" : "customers.pago_redeem / redeems",
        "tabla_destino": "flybackDW.factredeemsautorizados",
        "sleep_seg"    : 0,
    },
    {
        "sp"           : "CFB.spPagadosDW",
        "vista_origen" : "customers.pago_redeem / redeems",
        "tabla_destino": "flybackDW.factredeemspagados",
        "sleep_seg"    : 10,  # ← espera anti-deadlock después de Autorizados
    },
]

# ── Función reutilizable ─────────────────────────────────
def ejecutar_sp(tarea: dict):
    """
    Ejecuta un SP en MariaDB.
    Recibe el dict de configuración completo — agnóstico al SP.
    Si falla: registra en etl_audit_log y relanza la excepción.
    """
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


# ── Función log + email ──────────────────────────────────
def generar_log_y_notificar():
    mensaje = "\n".join([
        f"DAG: {DAG_ID} — INICIO",
        f"factredeemssolicitados — OK",
        f"factredeemsautorizados — OK",
        f"factredeemspagados     — OK",
        f"DAG: {DAG_ID} — FIN ✅",
    ])
    log_path = escribir_log_txt(LOG_PATH, "factRedeemsSolAutPag_mensual", mensaje)
    send_etl_notification(
        dag_id   = DAG_ID,
        status   = "OK",
        log_path = log_path,
    )


# ── DAG ─────────────────────────────────────────────────
with DAG(
    dag_id            = DAG_ID,
    description       = "Carga mensual de factredeemssolicitados, factredeemsautorizados y factredeemspagados en flybackDW",
    schedule_interval = "0 2 2 * *",   # ← Día 2 de cada mes a las 2:00am Cancún
    start_date        = pendulum.datetime(2026, 11, 2, tz="America/Cancun"),
    catchup           = False,
    tags              = ["flybackDW", "mariadb", "redeems"],
) as dag:

    cargar_sol = PythonOperator(
        task_id         = "cargar_factredeemssolicitados",
        python_callable = partial(ejecutar_sp, TAREAS[0]),
    )
    cargar_aut = PythonOperator(
        task_id         = "cargar_factredeemsautorizados",
        python_callable = partial(ejecutar_sp, TAREAS[1]),
    )
    cargar_pag = PythonOperator(
        task_id         = "cargar_factredeemspagados",
        python_callable = partial(ejecutar_sp, TAREAS[2]),
    )
    notificar = PythonOperator(
        task_id         = "generar_log_y_notificar",
        python_callable = generar_log_y_notificar,
    )

    # Secuencia: solicitados → autorizados → pagados → log + email
    cargar_sol >> cargar_aut >> cargar_pag >> notificar
