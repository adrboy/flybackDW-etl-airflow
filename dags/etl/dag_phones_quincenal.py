# ═══════════════════════════════════════════════════════
# dag_phones_quincenal.py
# Capa    : Negocio / Orquestador (DAG)
# Objetivo: Teléfonos quincenal
#           1) Silver: RAW MariaDB (tbl_raw_phoneXX) → source.PhoneXX
#              incremental, UPDATE + INSERT por conjuntos
#           2) Gold: PENDIENTE (SP sp_insert_phones_factPersonalInfo v1.2
#              + tarea con interruptor, igual que dag_clients_quincenal)
# Carpeta : etl/
# Versión : 1.0 — 2026-11-09 — BORRADOR pendiente de pruebas
#           Activas: las cinco — fb, bb, ml, fi, vc
#           (y a FUENTES_HABILITADAS en common/etl_silver_phone_da.py)
#           cuando sus tablas Silver se recreen.
#
# ── Orden de ejecución (en cadena) ──────────────────────
#   242 primero: fb → bb → ml     después 240: fi → vc
#   Si una tarea falla, las siguientes SIGUEN (trigger_rule all_done).
#   Cuando exista Gold, decidir_gold dependerá de TODAS las fuentes.
#
# ── Ejecución manual desde bash Docker ──────────────────
# Todas las fuentes habilitadas:
#   airflow dags trigger dag_phones_quincenal
#
# Una fuente específica:
#   airflow dags trigger dag_phones_quincenal --conf '{"fuente":"fb"}'
#
# Nota: se crea PAUSADO. Al activarlo, Airflow puede lanzar de
#       inmediato una corrida por el intervalo ya vencido
#       (comportamiento normal con catchup=False).
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from airflow                  import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions       import AirflowFailException
from datetime                 import datetime

from etl.etl_silver_phone_ne  import orquestar_silver_phone
from common.etl_silver_da     import SilverTransactionUnknownError

DAG_ID = 'dag_phones_quincenal'

# Orden de la cadena: 242 primero (fb, bb, ml) y después 240 (fi, vc).
FUENTES = [
    'fb'
  , 'bb'
  , 'ml'
  , 'fi'
  , 'vc'
]


# ════════════════════════════════════════════════════════
# Wrapper común — mismo contrato que dag_clients_quincenal
# ════════════════════════════════════════════════════════

def _tarea_silver_phone(fuente: str, **context) -> None:
    """
    Ejecuta Silver de teléfonos para una fuente.

    Se salta (sin error) cuando se pidió otra fuente (conf fuente).

    Comportamiento ante errores:
      False (PreWrite o Rollback) → Exception → Airflow reintenta
      SilverTransactionUnknownError → AirflowFailException
                                      → SIN reintento
    """
    dag_run = context.get('dag_run')
    conf = (dag_run.conf if dag_run else None) or {}
    fuente_solicitada = conf.get('fuente', None)

    if fuente_solicitada and fuente_solicitada != fuente:
        print(f"Fuente {fuente_solicitada} solicitada — saltando {fuente}")
        return

    try:
        resultado = orquestar_silver_phone(fuente)
    except SilverTransactionUnknownError as e:
        raise AirflowFailException(
            f"Silver teléfonos {fuente.upper()} — estado desconocido, "
            f"revisar manualmente: {e}"
        ) from e

    if not resultado:
        raise Exception(
            f"Silver teléfonos {fuente.upper()} falló — Silver intacto, revisar log."
        )


# ════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════

with DAG(
    dag_id                    = DAG_ID
  , description               = (
        'Teléfonos quincenal — Silver RAW → SQL Server '
        '(cadena fb/bb/ml/fi/vc, días 1 y 15, 7am); Gold pendiente'
    )
  , schedule_interval         = '0 7 1,15 * *'
  , start_date                = datetime(2026, 11, 1)
  , catchup                   = False
  , max_active_runs           = 1
  , is_paused_upon_creation   = True
  , tags                      = ['telefonos', 'quincenal', 'silver']
  , params                    = {'fuente': None}
) as dag:

    tareas = []

    for f in FUENTES:
        tarea = PythonOperator(
            task_id         = f'silver_phone_{f}'
          , python_callable = _tarea_silver_phone
          , op_kwargs       = {'fuente': f}
          , retries         = 1       # ← reintenta en PreWrite o Rollback
          , retry_delay     = 300
          , trigger_rule    = 'all_done' if tareas else 'all_success'
        )
        if tareas:
            tareas[-1] >> tarea
        tareas.append(tarea)

    # ── Cadena ───────────────────────────────────────────
    #
    #  silver_phone_fb ─► silver_phone_bb ─► silver_phone_ml
    #                                           ─► silver_phone_fi ─► silver_phone_vc
    #
    #  Fallo Normal     → reintenta (retries=1)
    #  Estado Unknown   → AirflowFailException, sin reintento
    #  Una tarea fallida NO detiene a las siguientes.
    #
    #  GOLD (pendiente): decidir_gold >> gold_phones
    #  decidir_gold dependerá de TODAS las tareas Silver.
