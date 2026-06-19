# ═══════════════════════════════════════════════════════
# DAG: dag_clientsbb_242
# Objetivo: ETL clientes BB desde MariaDB 242 → SQL Server
# Carpeta: etl/
# Versión: 2.0 — 2026-06-19 (SQL externalizado + executemany)
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
VISTA_ORIGEN  = "db_general.viewclientsbb"
TABLA_DESTINO = "source.clientsbb"
SQL_SELECT    = "sql/clients/select_clientsbb_242.sql"
SQL_INSERT    = "sql/clients/insert_clientsbb_242.sql"

# ── Función ETL ──────────────────────────────────────────
def etl_clientsbb():
    fecha_inicio = datetime.now()
    mensaje_log  = []
    max_id = 0
    filas  = 0
    estado = "ERROR"
    mensaje_error = None

    try:
        # Paso 1 — MAX clientid del destino
        max_id = get_max_id(MSSQL_CONN_ID, TABLA_DESTINO)
        mensaje_log.append(f"MAX clientid destino: {max_id}")

        # Paso 2 — ETL con SQL externo + executemany
        filas = ejecutar_insert(
            dag_id          = "dag_clientsbb_242"
          , mariadb_conn_id = ORIGEN_CONN_ID_242
          , mssql_conn_id   = MSSQL_CONN_ID
          , sql_select      = SQL_SELECT
          , sql_insert      = SQL_INSERT
          , max_id          = max_id
        )
        mensaje_log.append(f"Filas insertadas: {filas}")
        estado = "SUCCESS"

    except Exception as e:
        mensaje_log.append(f"ERROR: {str(e)}")
        mensaje_error = str(e)
        raise

    finally:
        try:
            registrar_log(
                paquete       = "etl_clientsbb_242"
              , vista_origen  = VISTA_ORIGEN
              , tabla_destino = TABLA_DESTINO
              , max_id_inicio = max_id
              , filas_insertadas = filas
              , tipo_ejecucion   = "SCHEDULED"
              , estado           = estado
              , mensaje_error    = mensaje_error
              , fecha_inicio     = fecha_inicio
              , fecha_fin        = datetime.now()
            )
            escribir_log_txt(LOG_PATH, "clientsbb", "\n".join(mensaje_log))
        except Exception as log_error:
            print(f"WARNING: Log falló pero ETL fue exitoso: {str(log_error)}")

# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id            = "dag_clientsbb_242"
  , start_date        = datetime(2026, 1, 1)
  , schedule_interval = None
  , catchup           = False
  , tags              = ["bronze", "242", "clientsbb"]
) as dag:

    tarea_etl = PythonOperator(
        task_id         = "etl_clientsbb"
      , python_callable = etl_clientsbb
      , retries         = 3
      , retry_delay     = 60
    )
