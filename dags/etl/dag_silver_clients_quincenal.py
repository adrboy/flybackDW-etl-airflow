# ═══════════════════════════════════════════════════════
# dag_silver_clients_quincenal.py
# Capa    : Negocio / Orquestador (DAG)
# Objetivo: INSERT + UPDATE Silver de clientes
#           RAW MariaDB → source SQL Server
#           Días 1 y 15 de cada mes a las 6am
#           Todas las fuentes en PARALELO
# Carpeta : etl/
# Versión : 1.1 — 2026-09-21
#   v1.0: solo fb
#   v1.1: fb + bb + ml en paralelo; soporte ejecución manual PowerShell
#
# ── Ejecución manual desde PowerShell ───────────────────
# Todas las fuentes:
#   docker exec -it airflow_scheduler airflow dags trigger dag_silver_clients_quincenal
#
# Una fuente específica:
#   docker exec -it airflow_scheduler airflow dags trigger dag_silver_clients_quincenal --conf '{"fuente":"fb"}'
#   docker exec -it airflow_scheduler airflow dags trigger dag_silver_clients_quincenal --conf '{"fuente":"bb"}'
#   docker exec -it airflow_scheduler airflow dags trigger dag_silver_clients_quincenal --conf '{"fuente":"ml"}'
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from airflow                  import DAG
from airflow.operators.python import PythonOperator
from datetime                 import datetime

from etl.etl_silver_ne        import orquestar_silver

DAG_ID = 'dag_silver_clients_quincenal'


# ════════════════════════════════════════════════════════
# Wrappers para PythonOperator — uno por fuente
# ════════════════════════════════════════════════════════

def task_silver_fb(**context):
    """Silver fb — source.clientsfb"""
    fuente = context.get('dag_run').conf.get('fuente', None)
    if fuente and fuente != 'fb':
        print(f"[{datetime.now()}] Fuente {fuente} solicitada — saltando fb")
        return
    resultado = orquestar_silver('fb')
    if not resultado:
        raise Exception('Silver FB falló — revisar log.')


def task_silver_bb(**context):
    """Silver bb — source.clientsbb"""
    fuente = context.get('dag_run').conf.get('fuente', None)
    if fuente and fuente != 'bb':
        print(f"[{datetime.now()}] Fuente {fuente} solicitada — saltando bb")
        return
    resultado = orquestar_silver('bb')
    if not resultado:
        raise Exception('Silver BB falló — revisar log.')


def task_silver_ml(**context):
    """Silver ml — source.clientsml"""
    fuente = context.get('dag_run').conf.get('fuente', None)
    if fuente and fuente != 'ml':
        print(f"[{datetime.now()}] Fuente {fuente} solicitada — saltando ml")
        return
    resultado = orquestar_silver('ml')
    if not resultado:
        raise Exception('Silver ML falló — revisar log.')


# ════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════

with DAG(
    dag_id            = DAG_ID
  , description       = 'Silver quincenal RAW → SQL Server — fb/bb/ml en paralelo (días 1 y 15, 6am)'
  , schedule_interval = '0 6 1,15 * *'
  , start_date        = datetime(2026, 9, 19)
  , catchup           = False
  , tags              = ['silver', 'quincenal', 'clientes']
  , params            = {'fuente': None}   # ← parámetro para ejecución manual
) as dag:

    # ── Tasks en PARALELO ────────────────────────────────
    tarea_fb = PythonOperator(
        task_id         = 'silver_fb'
      , python_callable = task_silver_fb
      , provide_context = True
      , retries         = 1
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

    # ── PARALELO — sin dependencia entre fuentes ─────────
    #
    #  silver_fb ──┐
    #  silver_bb ──┼──► (fin — cada una reporta independiente)
    #  silver_ml ──┘
    #
    [tarea_fb, tarea_bb, tarea_ml]
