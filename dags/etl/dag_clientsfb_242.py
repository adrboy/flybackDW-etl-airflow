# ═══════════════════════════════════════════════════════
# DAG: dag_clientsfb_242
# Objetivo: ETL clientes FB desde MariaDB 242 → SQL Server
# Carpeta: etl/
# Versión: 2.0 — 2026-06-22 (SQL externalizado + executemany)
# ═══════════════════════════════════════════════════════

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime                  import datetime
import sys
sys.path.insert(0, '/opt/airflow/dags')
from common.etl_base      import get_max_id, ejecutar_insert
from common.audit_logger  import registrar_log, escribir_log_txt
from common.db_connections import (
    ORIGEN_CONN_ID_242
  , MSSQL_CONN_ID
  , LOG_PATH
)

# ── Configuración ────────────────────────────────────────
VISTA_ORIGEN  = "db_general.viewclientsfb"
TABLA_DESTINO = "source.clientsfb"
SQL_SELECT    = "sql/clients/select_clientsfb_242.sql"
SQL_INSERT    = "sql/clients/insert_clientsfb_242.sql"

# ── Función ETL ──────────────────────────────────────────
def etl_clientsfb():
    fecha_inicio  = datetime.now()
    mensaje_log   = []
    max_id        = 0
    filas         = 0
    estado        = "ERROR"
    mensaje_error = None

    try:
        # Paso 1 — MAX clientid del destino
        max_id = get_max_id(MSSQL_CONN_ID, TABLA_DESTINO)
        mensaje_log.append(f"[DAG: dag_clientsfb_242] MAX clientid destino: {max_id}")

        # Paso 2 — ETL con SQL externo + executemany
        filas = ejecutar_insert(
            dag_id          = "dag_clientsfb_242"
          , mariadb_conn_id = ORIGEN_CONN_ID_242
          , mssql_conn_id   = MSSQL_CONN_ID
          , sql_select      = SQL_SELECT
          , sql_insert      = SQL_INSERT
          , max_id          = max_id
        )
        mensaje_log.append(f"[DAG: dag_clientsfb_242] Filas insertadas: {filas}")
        estado = "SUCCESS"

    except Exception as e:
        mensaje_log.append(f"[DAG: dag_clientsfb_242] ERROR: {str(e)}")
        mensaje_error = str(e)
        raise

    finally:
        try:
            registrar_log(
                paquete          = "etl_clientsfb_242"
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
            escribir_log_txt(LOG_PATH, "clientsfb", "\n".join(mensaje_log))
        except Exception as log_error:
            print(f"WARNING: Log falló pero ETL fue exitoso: {str(log_error)}")

# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id            = "dag_clientsfb_242"
  , start_date        = datetime(2026, 1, 1)
  , schedule_interval = None
  , catchup           = False
  , tags              = ["bronze", "242", "clientsfb"]
) as dag:

    tarea_etl = PythonOperator(
        task_id         = "etl_clientsfb"
      , python_callable = etl_clientsfb
      , retries         = 3
      , retry_delay     = 60
    )
