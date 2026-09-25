# ═══════════════════════════════════════════════════════
# dag_silver_clients_quincenal.py
# Capa    : Negocio / Orquestador (DAG)
# Objetivo: INSERT + UPDATE Silver de clientes
#           RAW MariaDB → source SQL Server
#           Días 1 y 15 de cada mes a las 6am
#           Las 5 fuentes en PARALELO
# Carpeta : etl/
# Versión : 1.3 — 2026-09-24
#   v1.0: solo fb
#   v1.1: fb + bb + ml en paralelo
#   v1.2: fb + bb + ml + fi + vc — las 5 fuentes completas
#   v1.3: SilverTransactionUnknownError → AirflowFailException
#         sin reintento — estado desconocido requiere revisión manual
#         PreWrite y Rollback → Exception normal con retries=1
#
# ── Ejecución manual desde bash Docker ──────────────────
# Todas las fuentes:
#   airflow dags trigger dag_silver_clients_quincenal
#
# Una fuente específica:
#   airflow dags trigger dag_silver_clients_quincenal --conf '{"fuente":"fb"}'
#   airflow dags trigger dag_silver_clients_quincenal --conf '{"fuente":"bb"}'
#   airflow dags trigger dag_silver_clients_quincenal --conf '{"fuente":"ml"}'
#   airflow dags trigger dag_silver_clients_quincenal --conf '{"fuente":"fi"}'
#   airflow dags trigger dag_silver_clients_quincenal --conf '{"fuente":"vc"}'
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from airflow                        import DAG
from airflow.operators.python       import PythonOperator
from airflow.exceptions             import AirflowFailException
from datetime                       import datetime

from etl.etl_silver_ne              import orquestar_silver
from common.etl_silver_da           import SilverTransactionUnknownError

DAG_ID = 'dag_silver_clients_quincenal'


# ════════════════════════════════════════════════════════
# Helper — wrapper común para todas las fuentes
# ════════════════════════════════════════════════════════

def _ejecutar_fuente(fuente: str, context: dict) -> None:
    """
    Ejecuta orquestar_silver para una fuente.

    Comportamiento ante errores:
      False (PreWrite o Rollback) → Exception → Airflow reintenta
      SilverTransactionUnknownError → AirflowFailException
                                      → SIN reintento
    """
    fuente_solicitada = context.get('dag_run').conf.get('fuente', None)
    if fuente_solicitada and fuente_solicitada != fuente:
        print(f"Fuente {fuente_solicitada} solicitada — saltando {fuente}")
        return

    try:
        resultado = orquestar_silver(fuente)
    except SilverTransactionUnknownError as e:
        # Estado desconocido — bloquear reintento
        raise AirflowFailException(
            f"Silver {fuente.upper()} — estado desconocido, "
            f"revisar manualmente: {e}"
        ) from e

    if not resultado:
        # PreWrite o Rollback confirmado — Airflow reintenta
        raise Exception(
            f"Silver {fuente.upper()} falló — Silver intacto, revisar log."
        )


# ════════════════════════════════════════════════════════
# Wrappers — uno por fuente
# ════════════════════════════════════════════════════════

def task_silver_fb(**context):
    """Silver fb — source.clientsfb (instancia 242)"""
    _ejecutar_fuente('fb', context)


def task_silver_bb(**context):
    """Silver bb — source.clientsbb (instancia 242)"""
    _ejecutar_fuente('bb', context)


def task_silver_ml(**context):
    """Silver ml — source.clientsml (instancia 242)"""
    _ejecutar_fuente('ml', context)


def task_silver_fi(**context):
    """Silver fi — source.clientsfi (instancia 240)"""
    _ejecutar_fuente('fi', context)


def task_silver_vc(**context):
    """Silver vc — source.clientsvc (instancia 240)"""
    _ejecutar_fuente('vc', context)


# ════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════

with DAG(
    dag_id            = DAG_ID
  , description       = (
        'Silver quincenal RAW → SQL Server — '
        '5 fuentes en paralelo (días 1 y 15, 6am)'
    )
  , schedule_interval = '0 6 1,15 * *'
  , start_date        = datetime(2026, 9, 19)
  , catchup           = False
  , tags              = ['silver', 'quincenal', 'clientes']
  , params            = {'fuente': None}
) as dag:

    tarea_fb = PythonOperator(
        task_id         = 'silver_fb'
      , python_callable = task_silver_fb
      , provide_context = True
      , retries         = 1       # ← reintenta en PreWrite o Rollback
      , retry_delay     = 300
    )

    tarea_bb = PythonOperator(
        task_id         = 'silver_bb'
      , python_callable = task_silver_bb
      , provide_context = True
      , retries         = 1
      , retry_delay     = 300
    )

    tarea_ml = PythonOperator(
        task_id         = 'silver_ml'
      , python_callable = task_silver_ml
      , provide_context = True
      , retries         = 1
      , retry_delay     = 300
    )

    tarea_fi = PythonOperator(
        task_id         = 'silver_fi'
      , python_callable = task_silver_fi
      , provide_context = True
      , retries         = 1
      , retry_delay     = 300
    )

    tarea_vc = PythonOperator(
        task_id         = 'silver_vc'
      , python_callable = task_silver_vc
      , provide_context = True
      , retries         = 1
      , retry_delay     = 300
    )

    # ── 5 tasks PARALELAS — sin dependencia entre fuentes ─
    #
    #  silver_fb ──┐
    #  silver_bb ──┤  Fallo Normal     → reintenta (retries=1)
    #  silver_ml ──┼  Estado Unknown   → AirflowFailException
    #  silver_fi ──┤                     sin reintento
    #  silver_vc ──┘
    #
    [tarea_fb, tarea_bb, tarea_ml, tarea_fi, tarea_vc]
