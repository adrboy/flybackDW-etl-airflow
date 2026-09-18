# ═══════════════════════════════════════════════════════
# dag_raw_clients_delete_mensual.py
# Capa    : Negocio / Orquestador (DAG)
# Objetivo: Soft delete mensual de huérfanos en RAW
#           Instancias 242 y 240 en PARALELO
#           Día 3 de cada mes a las 3:00am
#           (día 1 = cierre de mes, día 2 = buffer)
# Carpeta : etl/
# Versión : 1.0 — 2026-09-18
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from airflow                  import DAG
from airflow.operators.python import PythonOperator
from datetime                 import datetime

from etl.etl_raw_delete_ne    import orquestar

DAG_ID = 'dag_raw_clients_delete_mensual'


# ════════════════════════════════════════════════════════
# Wrappers para PythonOperator
# ════════════════════════════════════════════════════════

def task_delete_242():
    """
    Soft delete mensual instancia 242:
      → db_general.etl_clientsfb_delete
      → db_general.etl_clientsbb_delete
      → db_general.etl_clientsml_delete
    """
    resultado = orquestar('242')
    if not resultado:
        raise Exception('Instancia 242 — al menos un SP DELETE falló. Revisar log.')


def task_delete_240():
    """
    Soft delete mensual instancia 240:
      → db_general.etl_clientsfi_delete
      → db_general.etl_clientsvc_delete
    """
    resultado = orquestar('240')
    if not resultado:
        raise Exception('Instancia 240 — al menos un SP DELETE falló. Revisar log.')


# ════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════

with DAG(
    dag_id            = DAG_ID
  , description       = 'Soft delete mensual RAW clientes — día 3 de cada mes 3am (242: fb/bb/ml | 240: fi/vc)'
  , schedule_interval = '0 3 3 * *'   # ← día 3 de cada mes a las 3am
  , start_date        = datetime(2026, 9, 18)
  , catchup           = False
  , tags              = ['raw', 'delete', 'mensual', 'clientes']
) as dag:

    # ── Task instancia 242 ───────────────────────────────
    tarea_delete_242 = PythonOperator(
        task_id         = 'delete_instancia_242'
      , python_callable = task_delete_242
      , retries         = 1
      , retry_delay     = 300   # ← 5 min entre reintentos
    )

    # ── Task instancia 240 ───────────────────────────────
    tarea_delete_240 = PythonOperator(
        task_id         = 'delete_instancia_240'
      , python_callable = task_delete_240
      , retries         = 1
      , retry_delay     = 300   # ← 5 min entre reintentos
    )

    # ── PARALELO — sin dependencia entre instancias ──────
    #
    #  delete_instancia_242 ──┐
    #                         ├──► (fin — cada una reporta independiente)
    #  delete_instancia_240 ──┘
    #
    [tarea_delete_242, tarea_delete_240]
