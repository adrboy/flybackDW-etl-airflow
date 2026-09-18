# ═══════════════════════════════════════════════════════
# dag_raw_clients_cdc_diario.py
# Capa    : Negocio / Orquestador (DAG)
# Objetivo: Ejecutar SPs CDC de las 5 RAW de clientes
#           Instancias 242 y 240 en PARALELO
#           8:00am y 6:00pm todos los días
# Carpeta : etl/
# Versión : 1.0 — 2026-09-17
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from airflow                  import DAG
from airflow.operators.python import PythonOperator
from datetime                 import datetime
from functools                import partial

from etl.etl_raw_ne           import orquestar

DAG_ID = 'dag_raw_clients_cdc_diario'

# ════════════════════════════════════════════════════════
# Wrappers para PythonOperator
# ════════════════════════════════════════════════════════

def task_instancia_242():
    """
    Ejecuta SPs CDC de la instancia 242:
      → db_general.etl_clientsfb
      → db_general.etl_clientsbb
      → db_general.etl_clientsml
    """
    resultado = orquestar('242')
    if not resultado:
        raise Exception('Instancia 242 — al menos un SP CDC falló. Revisar log.')


def task_instancia_240():
    """
    Ejecuta SPs CDC de la instancia 240:
      → db_general.etl_clientsfi
      → db_general.etl_clientsvc
    """
    resultado = orquestar('240')
    if not resultado:
        raise Exception('Instancia 240 — al menos un SP CDC falló. Revisar log.')


# ════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════

with DAG(
    dag_id            = DAG_ID
  , description       = 'CDC diario RAW clientes — 5 SPs en paralelo (242: fb/bb/ml | 240: fi/vc)'
  , schedule_interval = '0 8,18 * * *'   # ← 8am y 6pm todos los días
  , start_date        = datetime(2026, 9, 17)
  , catchup           = False
  , tags              = ['raw', 'cdc', 'diario', 'clientes']
) as dag:

    # ── Task instancia 242 ───────────────────────────────
    tarea_242 = PythonOperator(
        task_id         = 'cdc_instancia_242'
      , python_callable = task_instancia_242
      , retries         = 2
      , retry_delay     = 60
    )

    # ── Task instancia 240 ───────────────────────────────
    tarea_240 = PythonOperator(
        task_id         = 'cdc_instancia_240'
      , python_callable = task_instancia_240
      , retries         = 2
      , retry_delay     = 60
    )

    # ── PARALELO — sin dependencia entre instancias ──────
    #
    #  cdc_instancia_242 ──┐
    #                      ├──► (fin — cada una reporta independiente)
    #  cdc_instancia_240 ──┘
    #
    [tarea_242, tarea_240]
