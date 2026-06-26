# ═══════════════════════════════════════════════════════
# DAG: dag_tbInicioSolAutPag_diario
# Objetivo: Actualizar tblInicioSolicitados, tblInicioAutorizados
#           y tblInicioPagados en flybackDW
# Carpeta: etl_flyback/
# Versión: 3.0 — 2026-06-25 (función ejecutar_sp() reutilizable, sin SQL embebido)
# ═══════════════════════════════════════════════════════
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from datetime                 import datetime
from time                     import sleep
from functools                import partial
import sys
sys.path.insert(0, '/opt/airflow/dags')
from common.audit_logger   import escribir_log_txt
from common.email_notifier import send_etl_notification
from common.db_connections import LOG_PATH
from common.sql_loader     import cargar_sql

DAG_ID = "dag_tbInicioSolAutPag_diario"

# ── Configuración de tareas — única fuente de verdad ────
# Para agregar un SP nuevo: solo agrega un dict a esta lista
TAREAS = [
    {
        "sp"           : "flybackDW.update_flybackDW_tblInicioSolicitados_VI_hour",
        "vista_origen" : "customers.redeems",
        "tabla_destino": "flybackDW.tblInicioSolicitados",
        "sleep_seg"    : 0,
    },
    {
        "sp"           : "flybackDW.update_flybackDW_tblInicioAutorizados_VI_hour",
        "vista_origen" : "customers.pago_redeem / redeems",
        "tabla_destino": "flybackDW.tblInicioAutorizados",
        "sleep_seg"    : 0,
    },
    {
        "sp"           : "flybackDW.update_flybackDW_tblInicioPagados_VI_hour",
        "vista_origen" : "customers.pago_redeem / redeems",
        "tabla_destino": "flybackDW.tblInicioPagados",
        "sleep_seg"    : 10,   # ← espera anti-deadlock después de Autorizados
    },
]

# ── Función reutilizable ─────────────────────────────────
def ejecutar_sp(tarea: dict):
    """
    Ejecuta un SP en MariaDB.
    Recibe el dict de configuración completo — agnóstico al SP.
    Si falla: registra en etl_audit_log y relanza la excepción.
    """
    if tarea["sleep_seg"] > 0:
        sleep(tarea["sleep_seg"])

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
        f"tblInicioSolicitados — OK",
        f"tblInicioAutorizados — OK",
        f"tblInicioPagados     — OK",
        f"DAG: {DAG_ID} — FIN ✅",
    ])
    log_path = escribir_log_txt(LOG_PATH, "tbInicioSolAutPag", mensaje)
    send_etl_notification(
        dag_id   = DAG_ID,
        status   = "OK",
        log_path = log_path,
    )


# ── DAG ─────────────────────────────────────────────────
with DAG(
    dag_id            = DAG_ID,
    description       = "Actualiza tblInicioSolicitados, tblInicioAutorizados y tblInicioPagados en flybackDW",
    schedule_interval = "30 5 * * 1-5",   # ← Lunes a Viernes 5:30am Cancún
    start_date        = datetime(2026, 6, 26),  # ← actualizado para evitar catchup
    catchup           = False,
    tags              = ["flybackDW", "redeems", "mariadb"],
) as dag:

    actualizar_sol = PythonOperator(
        task_id         = "actualizar_tblInicioSolicitados",
        python_callable = partial(ejecutar_sp, TAREAS[0]),
    )
    actualizar_aut = PythonOperator(
        task_id         = "actualizar_tblInicioAutorizados",
        python_callable = partial(ejecutar_sp, TAREAS[1]),
    )
    actualizar_pag = PythonOperator(
        task_id         = "actualizar_tblInicioPagados",
        python_callable = partial(ejecutar_sp, TAREAS[2]),
    )
    notificar = PythonOperator(
        task_id         = "generar_log_y_notificar",
        python_callable = generar_log_y_notificar,
    )

    # Secuencia: sol → aut → pag → log + email
    actualizar_sol >> actualizar_aut >> actualizar_pag >> notificar
