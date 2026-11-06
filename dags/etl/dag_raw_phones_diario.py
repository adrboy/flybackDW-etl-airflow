# ═══════════════════════════════════════════════════════
# dag_raw_phones_diario.py
# Capa    : Negocio / Orquestador (DAG)
# Objetivo: Mantener al día el RAW de teléfonos de los 5 productos
#           ejecutando los SPs sp_raw_phone* (insertar nuevos,
#           revivir y marcar deletedAt con compuerta de seguridad)
#           Instancias 242 y 240 en PARALELO
#           Todos los días a las 5:00am
# Carpeta : etl/
# Versión : 1.1 — BORRADOR pendiente de pruebas
#   v1.0: nombre dag_raw_phones_mensual, primer lunes 5am, sin fb
#   v1.1: renombrado a dag_raw_phones_diario, diario 5am, fb incluido,
#         correo solo cuando falla (ver etl_raw_phone_ne.py)
# Nota    : Se crea PAUSADO. Activarlo manualmente en Airflow.
#           Al activarlo, Airflow puede lanzar de inmediato una
#           corrida por el intervalo ya vencido (comportamiento
#           normal de un DAG nuevo con catchup=False). Es segura:
#           los SPs son idempotentes y la compuerta protege el borrado.
#           Reintentos en 0: repetir un BLOQUEADO de la compuerta
#           no lo resuelve.
#
# ── Ejecución manual desde bash Docker ──────────────────
#   airflow dags trigger dag_raw_phones_diario
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from airflow                  import DAG
from airflow.operators.python import PythonOperator
from datetime                 import datetime

from etl.etl_raw_phone_ne     import orquestar

DAG_ID = 'dag_raw_phones_diario'


# ════════════════════════════════════════════════════════
# Wrappers para PythonOperator
# ════════════════════════════════════════════════════════

def task_instancia_242():
    """
    Ejecuta SPs RAW de teléfonos de la instancia 242:
      → db_general.sp_raw_phonefb
      → db_general.sp_raw_phonebb
      → db_general.sp_raw_phoneml
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
  , description           = 'RAW teléfonos — diario 5am (242: fb/bb/ml | 240: fi/vc)'
  , schedule_interval     = '0 5 * * *'   # ← todos los días a las 5:00
  , start_date            = datetime(2026, 11, 1)
  , catchup               = False
  , max_active_runs       = 1
  , is_paused_upon_creation = True
  , tags                  = ['raw', 'phones', 'diario']
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
