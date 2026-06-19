# ═══════════════════════════════════════════════════════
# DAG: dag_tbInicioSolAutPag_diario
# Objetivo: Actualizar tblInicioSolicitados, tblInicioAutorizados
#           y tblInicioPagados en flybackDW
# Carpeta: etl_flyback/
# Versión: 2.3 — 2026-06-19 (try/except en las 3 tareas + sleep anti-deadlock)
# ═══════════════════════════════════════════════════════
from airflow import DAG
from airflow.operators.python  import PythonOperator
from airflow.hooks.mysql_hook  import MySqlHook
from datetime                  import datetime
from time                      import sleep
import sys
sys.path.insert(0, '/opt/airflow/dags')
from common.audit_logger   import escribir_log_txt
from common.email_notifier import send_etl_notification
from common.db_connections import LOG_PATH

DAG_ID = "dag_tbInicioSolAutPag_diario"

# ── Funciones ETL ────────────────────────────────────────
def ejecutar_solicitados():
    hook = MySqlHook(mysql_conn_id='MariaDB')
    try:
        hook.run("CALL flybackDW.update_flybackDW_tblInicioSolicitados_VI_hour();")
        print(f"[{datetime.now()}] tblInicioSolicitados — OK")
    except Exception as e:
        hook.run(f"""
            INSERT INTO flybackDW.etl_audit_log
                   (paquete, vista_origen, tabla_destino, tipo_ejecucion, estado, mensaje_error, fecha_inicio, fecha_fin)
            VALUES ('update_flybackDW_tblInicioSolicitados_VI_hour',
                    'customers.redeems', 'flybackDW.tblInicioSolicitados',
                    'HORA', 'ERROR', '{str(e)[:500]}', NOW(), NOW())
        """)
        raise

def ejecutar_autorizados():
    hook = MySqlHook(mysql_conn_id='MariaDB')
    try:
        hook.run("CALL flybackDW.update_flybackDW_tblInicioAutorizados_VI_hour();")
        print(f"[{datetime.now()}] tblInicioAutorizados — OK")
    except Exception as e:
        hook.run(f"""
            INSERT INTO flybackDW.etl_audit_log
                   (paquete, vista_origen, tabla_destino, tipo_ejecucion, estado, mensaje_error, fecha_inicio, fecha_fin)
            VALUES ('update_flybackDW_tblInicioAutorizados_VI_hour',
                    'customers.pago_redeem / redeems', 'flybackDW.tblInicioAutorizados',
                    'HORA', 'ERROR', '{str(e)[:500]}', NOW(), NOW())
        """)
        raise

def ejecutar_pagados():
    sleep(10)  # ← espera 10s para que MariaDB libere locks de Autorizados
    hook = MySqlHook(mysql_conn_id='MariaDB')
    try:
        hook.run("CALL flybackDW.update_flybackDW_tblInicioPagados_VI_hour();")
        print(f"[{datetime.now()}] tblInicioPagados — OK")
    except Exception as e:
        hook.run(f"""
            INSERT INTO flybackDW.etl_audit_log
                   (paquete, vista_origen, tabla_destino, tipo_ejecucion, estado, mensaje_error, fecha_inicio, fecha_fin)
            VALUES ('update_flybackDW_tblInicioPagados_VI_hour',
                    'customers.pago_redeem / redeems', 'flybackDW.tblInicioPagados',
                    'HORA', 'ERROR', '{str(e)[:500]}', NOW(), NOW())
        """)
        raise

# ── Función log + email ───────────────────────────────────
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

# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id            = DAG_ID,
    description       = "Actualiza tblInicioSolicitados, tblInicioAutorizados y tblInicioPagados en flybackDW",
    schedule_interval = "30 5 * * 1-5",   # ← Lunes a Viernes a las 5:30am Cancún
    start_date        = datetime(2026, 6, 11),
    catchup           = False,
    tags              = ["flybackDW", "redeems", "mariadb"],
) as dag:

    actualizar_sol = PythonOperator(
        task_id         = "actualizar_tblInicioSolicitados",
        python_callable = ejecutar_solicitados,
    )
    actualizar_aut = PythonOperator(
        task_id         = "actualizar_tblInicioAutorizados",
        python_callable = ejecutar_autorizados,
    )
    actualizar_pag = PythonOperator(
        task_id         = "actualizar_tblInicioPagados",
        python_callable = ejecutar_pagados,
    )
    notificar = PythonOperator(
        task_id         = "generar_log_y_notificar",
        python_callable = generar_log_y_notificar,
    )

    # Secuencia: sol → aut → pag → log + email
    actualizar_sol >> actualizar_aut >> actualizar_pag >> notificar
