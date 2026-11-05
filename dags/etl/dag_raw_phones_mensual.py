# ═══════════════════════════════════════════════════════
# dag_raw_phones_mensual.py
# Capa    : Negocio / Orquestador (DAG)
# Objetivo: Ejecutar los SPs RAW de teléfonos
#           (insertar nuevos, revivir y marcar deletedAt)
#           Instancias 242 y 240 en PARALELO
#           Primer lunes de cada mes a las 5:00am
#           (una hora antes de dag_masterphones)
# Carpeta : etl/
# Versión : 1.0 — BORRADOR pendiente de pruebas
# Nota    : Se crea PAUSADO. Activarlo manualmente en Airflow.
#           Al activarlo, Airflow puede lanzar de inmediato una
#           corrida por el intervalo ya vencido (comportamiento
#           normal de un DAG nuevo con catchup=False).
#           Reintentos en 0: repetir un BLOQUEADO de la compuerta
#           no lo resuelve.
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from airflow                  import DAG
from airflow.operators.python import PythonOperator
from datetime                 import datetime

from etl.etl_raw_phone_ne     import orquestar

DAG_ID = 'dag_raw_phones_mensual'


# ════════════════════════════════════════════════════════
# Wrappers para PythonOperator
# ════════════════════════════════════════════════════════

def task_instancia_242():
    """
    Ejecuta SPs RAW de teléfonos de la instancia 242:
      → db_general.sp_raw_phonebb
      → db_general.sp_raw_phoneml
      (fb: pendiente de habilitar en common/etl_raw_phone_da.py)
    """
    resultado = orquestar('242')
    if not resultado:
        raise Exception('Instancia 242 — al menos un SP RAW de teléfonos falló o fue bloqueado. Revisar log.')


def task_instancia_240():
    """
    Ejecuta SPs RAW de teléfonos de la instancia 240:
      → db_general.sp_raw_phonefi
      → db_general.sp_raw_phonevc
    """
    resultado = orquestar('240')
    if not resultado:
        raise Exception('Instancia 240 — al menos un SP RAW de teléfonos falló o fue bloqueado. Revisar log.')


# ════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════

with DAG(
    dag_id                = DAG_ID
  , description           = 'RAW teléfonos — primer lunes 5am (242: bb/ml | 240: fi/vc)'
  , schedule_interval     = '0 5 * * 1#1'   # ← primer lunes de cada mes a las 5:00
  , start_date            = datetime(2026, 11, 1)
  , catchup               = False
  , max_active_runs       = 1
  , is_paused_upon_creation = True
  , tags                  = ['raw', 'phones', 'mensual']
) as dag:

    # ── Task instancia 242 ───────────────────────────────
    tarea_242 = PythonOperator(
        task_id         = 'raw_phones_instancia_242'
      , python_callable = task_instancia_242
      , retries         = 0
    )

    # ── Task instancia 240 ───────────────────────────────
    tarea_240 = PythonOperator(
        task_id         = 'raw_phones_instancia_240'
      , python_callable = task_instancia_240
      , retries         = 0
    )

    # ── PARALELO — sin dependencia entre instancias ──────
    #
    #  raw_phones_instancia_242 ──┐
    #                             ├──► (fin — cada una reporta independiente)
    #  raw_phones_instancia_240 ──┘
    #
    [tarea_242, tarea_240]
