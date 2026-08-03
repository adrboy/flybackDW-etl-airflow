# ═══════════════════════════════════════════════════════
# DAG : dag_datasync_master
# Objetivo : Sincronización mensual completa de clientes
#            GC + FB + BB + VTW → db_general
#
# Fase 1 (paralelo)  : gusa_collections, flyback,
#                      buyback, vacation_center
# Fase 2 (secuencial): generate_data → validate_country
#                      → log_estadisticas → notificar
#
# Schedule : primer lunes de cada mes a la 1:00 AM
#            (si el día 1 cae domingo se ejecuta el lunes)
# Carpeta  : etl_datasync/
# Versión  : 2.2 — 2026-07-31
# Cambios  : dag_id renombrado a dag_datasync_master
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from airflow                  import DAG
from airflow.operators.python import PythonOperator
from datetime                 import datetime

from common.audit_logger   import escribir_log_txt
from common.email_notifier import send_etl_notification
from common.db_connections import LOG_PATH

from etl_datasync.operations.gusa_collections import sincronizar_gusa_collections
from etl_datasync.operations.flyback          import sincronizar_flyback
from etl_datasync.operations.buyback          import sincronizar_buyback
from etl_datasync.operations.vacation_center  import sincronizar_vacation_center
from etl_datasync.operations.generate_data    import generar_data
from etl_datasync.operations.validate_country import validar_country
from etl_datasync.operations.log_estadisticas import registrar_estadisticas

DAG_ID = "dag_datasync_master"


# ── Wrappers para PythonOperator ────────────────────────

def task_gusa_collections():
    sincronizar_gusa_collections(DAG_ID)

def task_flyback():
    sincronizar_flyback(DAG_ID)

def task_buyback():
    sincronizar_buyback(DAG_ID)

def task_vacation_center():
    sincronizar_vacation_center(DAG_ID)

def task_generate_data():
    generar_data(DAG_ID)

def task_validate_country():
    validar_country(DAG_ID)

def task_log_estadisticas():
    registrar_estadisticas(DAG_ID)

def task_notificar():
    mensaje = "\n".join([
        f"DAG: {DAG_ID} — INICIO REPORTE",
        "Fase 1 (paralelo)  : gusa_collections, flyback, buyback, vacation_center",
        "Fase 2 (secuencial): generate_data → validate_country → log_estadisticas",
        f"DAG: {DAG_ID} — FIN ✅",
    ])
    log_path = escribir_log_txt(LOG_PATH, DAG_ID, mensaje)
    send_etl_notification(
        dag_id   = DAG_ID,
        status   = "OK",
        log_path = log_path,
    )


# ── DAG ─────────────────────────────────────────────────
with DAG(
    dag_id            = DAG_ID,
    description       = "Sincronización mensual GC+FB+BB+VTW → db_general — primer lunes de cada mes 1AM",
    schedule_interval = "0 1 * * MON#1",
    start_date        = datetime(2026, 8, 1),
    catchup           = False,
    tags              = ["datasync", "mensual", "db_general"],
) as dag:

    # ── Fase 1: paralelo ────────────────────────────────
    sync_gusa = PythonOperator(
        task_id         = "sync_gusa_collections",
        python_callable = task_gusa_collections,
    )
    sync_fb = PythonOperator(
        task_id         = "sync_flyback",
        python_callable = task_flyback,
    )
    sync_bb = PythonOperator(
        task_id         = "sync_buyback",
        python_callable = task_buyback,
    )
    sync_vtw = PythonOperator(
        task_id         = "sync_vacation_center",
        python_callable = task_vacation_center,
    )

    # ── Fase 2: secuencial ──────────────────────────────
    gen_data = PythonOperator(
        task_id         = "generate_data",
        python_callable = task_generate_data,
    )
    val_country = PythonOperator(
        task_id         = "validate_country",
        python_callable = task_validate_country,
    )
    log_stats = PythonOperator(
        task_id         = "log_estadisticas",
        python_callable = task_log_estadisticas,
    )
    notificar = PythonOperator(
        task_id         = "notificar",
        python_callable = task_notificar,
    )

    # ── Dependencias ────────────────────────────────────
    #
    #  sync_gusa  ──┐
    #  sync_fb    ──┤
    #               ├──► generate_data ──► validate_country ──► log_estadisticas ──► notificar
    #  sync_bb    ──┤
    #  sync_vtw   ──┘
    #
    [sync_gusa, sync_fb, sync_bb, sync_vtw] >> gen_data >> val_country >> log_stats >> notificar
