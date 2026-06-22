# ═══════════════════════════════════════════════════════
# DAG: dag_phonebb_242
# Objetivo: ETL phones BB desde MariaDB 242 → SQL Server
# Carpeta: etl/
# Versión: 2.1 — 2026-06-22 (dag_id → etl_basephone v2)
# ═══════════════════════════════════════════════════════

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime                  import datetime
import sys
sys.path.insert(0, '/opt/airflow/dags')
from common.etl_basephone  import ejecutar_truncate_insert
from common.audit_logger   import registrar_log, escribir_log_txt
from common.db_connections import (
    ORIGEN_CONN_ID_242
  , MSSQL_CONN_ID
  , LOG_PATH
)

# ── Configuración ────────────────────────────────────────
VISTA_ORIGEN  = "db_general.vwpersonalinfobb"
TABLA_DESTINO = "source.Phonebb"

# ── Función ETL ──────────────────────────────────────────
def etl_phonebb_242():
    fecha_inicio  = datetime.now()
    mensaje_log   = []
    filas         = 0
    estado        = "ERROR"
    mensaje_error = None

    try:
        filas = ejecutar_truncate_insert(
            dag_id          = "dag_phonebb_242"
          , mariadb_conn_id = ORIGEN_CONN_ID_242
          , mssql_conn_id   = MSSQL_CONN_ID
          , vista_origen    = VISTA_ORIGEN
          , tabla_destino   = TABLA_DESTINO
        )
        mensaje_log.append(f"[DAG: dag_phonebb_242] TRUNCATE + INSERT exitoso")
        mensaje_log.append(f"[DAG: dag_phonebb_242] Filas insertadas: {filas}")
        estado = "SUCCESS"

    except Exception as e:
        mensaje_log.append(f"[DAG: dag_phonebb_242] ERROR: {str(e)}")
        mensaje_error = str(e)
        raise

    finally:
        try:
            registrar_log(
                paquete          = "dag_phonebb_242"
              , vista_origen     = VISTA_ORIGEN
              , tabla_destino    = TABLA_DESTINO
              , max_id_inicio    = 0
              , filas_insertadas = filas
              , tipo_ejecucion   = "SCHEDULED"
              , estado           = estado
              , mensaje_error    = mensaje_error
              , fecha_inicio     = fecha_inicio
              , fecha_fin        = datetime.now()
            )
            escribir_log_txt(LOG_PATH, "phonebb", "\n".join(mensaje_log))
        except Exception as log_error:
            print(f"WARNING: Log falló pero ETL fue exitoso: {str(log_error)}")

# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id            = "dag_phonebb_242"
  , start_date        = datetime(2026, 1, 1)
  , schedule_interval = None
  , catchup           = False
  , tags              = ["bronze", "242", "phonebb"]
) as dag:

    tarea_etl = PythonOperator(
        task_id         = "etl_phonebb_242"
      , python_callable = etl_phonebb_242
      , retries         = 3
      , retry_delay     = 60
    )
