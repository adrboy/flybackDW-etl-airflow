# ═══════════════════════════════════════════════════════
# DAG: dag_clientsvc_240
# Objetivo: ETL clientes VC desde MariaDB 240 → SQL Server
# Carpeta: etl/
# Versión: 3.0 — 2026-08-24
#   - Integración error_classifier — reporte estructurado
#   - Log .txt y email automático SUCCESS/FAILED
#   - Referencia Airflow automática en reporte de error
# ═══════════════════════════════════════════════════════
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime                  import datetime
import sys
sys.path.insert(0, '/opt/airflow/dags')
from common.etl_base       import get_max_id, ejecutar_insert
from common.audit_logger   import registrar_log, escribir_log_txt
from common.db_connections import (
    ORIGEN_CONN_ID_240
  , MSSQL_CONN_ID
  , LOG_PATH
)

# ── Configuración ────────────────────────────────────────
DAG_ID        = "dag_clientsvc_240"
VISTA_ORIGEN  = "db_general.viewclientsvc"
TABLA_DESTINO = "source.clientsvc"
SQL_SELECT    = "sql/clients/select_clientsvc_240.sql"
SQL_INSERT    = "sql/clients/insert_clientsvc_240.sql"

# ── Función ETL ──────────────────────────────────────────
def etl_clientsvc(**context):
    fecha_inicio  = datetime.now()
    max_id        = 0
    filas         = 0
    estado        = "ERROR"
    mensaje_error = None
    reporte       = ""

    try:
        max_id = get_max_id(MSSQL_CONN_ID, TABLA_DESTINO)

        filas, reporte = ejecutar_insert(
            dag_id          = DAG_ID
          , mariadb_conn_id = ORIGEN_CONN_ID_240
          , mssql_conn_id   = MSSQL_CONN_ID
          , sql_select      = SQL_SELECT
          , sql_insert      = SQL_INSERT
          , max_id          = max_id
          , vista_origen    = VISTA_ORIGEN
          , tabla_destino   = TABLA_DESTINO
          , airflow_context = context
        )
        estado = "SUCCESS"

    except Exception as e:
        mensaje_error = str(e)
        raise

    finally:
        try:
            registrar_log(
                paquete          = DAG_ID
              , vista_origen     = VISTA_ORIGEN
              , tabla_destino    = TABLA_DESTINO
              , max_id_inicio    = max_id
              , filas_insertadas = filas
              , tipo_ejecucion   = "SCHEDULED"
              , estado           = estado
              , mensaje_error    = mensaje_error
              , fecha_inicio     = fecha_inicio
              , fecha_fin        = datetime.now()
            )
            escribir_log_txt(
                log_path  = LOG_PATH
              , vista     = "clientsvc"
              , reporte   = reporte
              , dag_id    = DAG_ID
              , estado    = estado
              , notificar = True
            )
        except Exception as log_error:
            print(f"WARNING: Log falló: {str(log_error)}")

# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id            = DAG_ID
  , start_date        = datetime(2026, 1, 1)
  , schedule_interval = None
  , catchup           = False
  , tags              = ["bronze", "240", "clientsvc"]
) as dag:

    tarea_etl = PythonOperator(
        task_id         = "etl_clientsvc"
      , python_callable = etl_clientsvc
      , retries         = 3
      , retry_delay     = 60
      , provide_context = True
    )
