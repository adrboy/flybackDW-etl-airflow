# ═══════════════════════════════════════════════════════
# dag_silver_clients_quincenal.py
# Capa    : Negocio / Orquestador (DAG)
# Objetivo: INSERT + UPDATE Silver de clientes
#           RAW MariaDB → source SQL Server
#           Días 1 y 15 de cada mes a las 6am
# Carpeta : etl/
# Versión : 1.0 — 2026-09-19
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from airflow                  import DAG
from airflow.operators.python import PythonOperator
from datetime                 import datetime

from etl.etl_silver_ne        import orquestar_silver

DAG_ID = 'dag_silver_clients_quincenal'


# ════════════════════════════════════════════════════════
# Wrappers para PythonOperator
# ════════════════════════════════════════════════════════

def task_silver_242():
    """
    Silver quincenal instancia 242:
      → source.clientsfb  (INSERT + UPDATE)
      ← tbl_raw_clientsfb
    """
    resultado = orquestar_silver('242')
    if not resultado:
        raise Exception('Silver 242 — al menos una operación falló. Revisar log.')


def task_silver_240():
    """
    Silver quincenal instancia 240:
      → source.clientsfi / source.clientsvc  (INSERT + UPDATE)
      ← tbl_raw_clientsfi / tbl_raw_clientsvc
    """
    resultado = orquestar_silver('240')
    if not resultado:
        raise Exception('Silver 240 — al menos una operación falló. Revisar log.')


# ════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════

with DAG(
    dag_id            = DAG_ID
  , description       = 'Silver quincenal RAW → SQL Server (días 1 y 15, 6am)'
  , schedule_interval = '0 6 1,15 * *'   # ← días 1 y 15 de cada mes a las 6am
  , start_date        = datetime(2026, 9, 19)
  , catchup           = False
  , tags              = ['silver', 'quincenal', 'clientes']
) as dag:

    # ── Task instancia 242 ───────────────────────────────
    tarea_silver_242 = PythonOperator(
        task_id         = 'silver_instancia_242'
      , python_callable = task_silver_242
      , retries         = 1
      , retry_delay     = 300   # ← 5 min entre reintentos
    )

    # ── Task instancia 240 ───────────────────────────────
    tarea_silver_240 = PythonOperator(
        task_id         = 'silver_instancia_240'
      , python_callable = task_silver_240
      , retries         = 1
      , retry_delay     = 300
    )

    # ── PARALELO ─────────────────────────────────────────
    #
    #  silver_instancia_242 ──┐
    #                         ├──► (fin — cada una reporta independiente)
    #  silver_instancia_240 ──┘
    #
    [tarea_silver_242, tarea_silver_240]
