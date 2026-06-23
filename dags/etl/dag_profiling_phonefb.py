# ═══════════════════════════════════════════════════════
# DAG: dag_profiling_phonefb
# Objetivo: Prueba de profiling contra tabla temporal
# Carpeta: etl/
# Versión: 1.0 — 2026-06-23
# IMPORTANTE: Solo para testing — NO tocar tabla real Phonefb
# ═══════════════════════════════════════════════════════

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime                  import datetime
import sys
sys.path.insert(0, '/opt/airflow/dags')
from common.etl_basephone  import ejecutar_truncate_insert
from common.db_connections import (
    ORIGEN_CONN_ID_242
  , MSSQL_CONN_ID
)

# ── Configuración — tabla TEMPORAL de prueba ─────────────
VISTA_ORIGEN  = "db_general.vwpersonalinfofb"
TABLA_DESTINO = "source.Phonefb_temp23062026"   # ← temporal, no la real

# ── Función ETL ──────────────────────────────────────────
def etl_profiling_phonefb():
    filas = ejecutar_truncate_insert(
        dag_id          = "dag_profiling_phonefb"
      , mariadb_conn_id = ORIGEN_CONN_ID_242
      , mssql_conn_id   = MSSQL_CONN_ID
      , vista_origen    = VISTA_ORIGEN
      , tabla_destino   = TABLA_DESTINO
    )
    return filas

# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id            = "dag_profiling_phonefb"
  , start_date        = datetime(2026, 1, 1)
  , schedule_interval = None
  , catchup           = False
  , tags              = ["test", "profiling", "phonefb"]
) as dag:

    tarea_etl = PythonOperator(
        task_id         = "etl_profiling_phonefb"
      , python_callable = etl_profiling_phonefb
      , retries         = 0   # ← sin reintentos en prueba
    )
