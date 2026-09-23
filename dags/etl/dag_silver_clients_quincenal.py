# ═══════════════════════════════════════════════════════
# dag_silver_clients_quincenal.py
# Capa    : Negocio / Orquestador (DAG)
# Objetivo: INSERT + UPDATE Silver de clientes
#           RAW MariaDB → source SQL Server
#           Días 1 y 15 de cada mes a las 6am
#           Las 5 fuentes en PARALELO
# Carpeta : etl/
# Versión : 1.2 — 2026-09-22
#   v1.0: solo fb
#   v1.1: fb + bb + ml en paralelo
#   v1.2: fb + bb + ml + fi + vc — las 5 fuentes completas
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

from airflow                  import DAG
from airflow.operators.python import PythonOperator
from datetime                 import datetime

from etl.etl_silver_ne        import orquestar_silver

DAG_ID = 'dag_silver_clients_quincenal'


# ════════════════════════════════════════════════════════
# Wrappers — uno por fuente
# ════════════════════════════════════════════════════════

def task_silver_fb(**context):
    """Silver fb — source.clientsfb (instancia 242)"""
    fuente = context.get('dag_run').conf.get('fuente', None)
    if fuente and fuente != 'fb':
        print(f"Fuente {fuente} solicitada — saltando fb")
        return
    if not orquestar_silver('fb'):
        raise Exception('Silver FB falló — revisar log.')


def task_silver_bb(**context):
    """Silver bb — source.clientsbb (instancia 242)"""
    fuente = context.get('dag_run').conf.get('fuente', None)
    if fuente and fuente != 'bb':
        print(f"Fuente {fuente} solicitada — saltando bb")
        return
    if not orquestar_silver('bb'):
        raise Exception('Silver BB falló — revisar log.')


def task_silver_ml(**context):
    """Silver ml — source.clientsml (instancia 242)"""
    fuente = context.get('dag_run').conf.get('fuente', None)
    if fuente and fuente != 'ml':
        print(f"Fuente {fuente} solicitada — saltando ml")
        return
    if not orquestar_silver('ml'):
        raise Exception('Silver ML falló — revisar log.')


def task_silver_fi(**context):
    """Silver fi — source.clientsfi (instancia 240)"""
    fuente = context.get('dag_run').conf.get('fuente', None)
    if fuente and fuente != 'fi':
        print(f"Fuente {fuente} solicitada — saltando fi")
        return
    if not orquestar_silver('fi'):
        raise Exception('Silver FI falló — revisar log.')


def task_silver_vc(**context):
    """Silver vc — source.clientsvc (instancia 240)"""
    fuente = context.get('dag_run').conf.get('fuente', None)
    if fuente and fuente != 'vc':
        print(f"Fuente {fuente} solicitada — saltando vc")
        return
    if not orquestar_silver('vc'):
        raise Exception('Silver VC falló — revisar log.')


# ════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════

with DAG(
    dag_id            = DAG_ID
  , description       = 'Silver quincenal RAW → SQL Server — 5 fuentes en paralelo (días 1 y 15, 6am)'
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
    #  silver_bb ──┤
    #  silver_ml ──┼──► (fin — cada una reporta independiente)
    #  silver_fi ──┤
    #  silver_vc ──┘
    #
    [tarea_fb, tarea_bb, tarea_ml, tarea_fi, tarea_vc]
