# ═══════════════════════════════════════════════════════
# dag_clients_quincenal.py
# (antes: dag_silver_clients_quincenal)
# Capa    : Negocio / Orquestador (DAG)
# Objetivo: Clientes quincenal
#           1) Silver: INSERT + UPDATE RAW MariaDB → source SQL Server
#              Días 1 y 15 de cada mes a las 6am, 5 fuentes en PARALELO
#           2) Gold: al terminar las 5 OK → sp_etl_maestro
#              detrás de un interruptor APAGADO por defecto
# Carpeta : etl/
# Versión : 1.5 — 2026-11-06
#   v1.0: solo fb
#   v1.1: fb + bb + ml en paralelo
#   v1.2: fb + bb + ml + fi + vc — las 5 fuentes completas
#   v1.3: SilverTransactionUnknownError → AirflowFailException
#         sin reintento — estado desconocido requiere revisión manual
#         PreWrite y Rollback → Exception normal con retries=1
#   v1.4: tarea final Gold clientes: EXEC [dw_etl].[sp_etl_maestro]
#         (conexión MSSQL244, igual que dag_master_gold).
#         Solo corre si: (1) las 5 fuentes terminaron OK,
#                        (2) no se pidió una fuente concreta,
#                        (3) el interruptor gold está encendido.
#         Interruptor: GOLD_HABILITADO_POR_DEFECTO = False
#         → en las corridas programadas Gold se SALTA.
#   v1.5: renombrado: dag_silver_clients_quincenal → dag_clients_quincenal
#         (el DAG ya no es solo Silver). Nueva opción solo_gold: ejecuta
#         únicamente Gold, sin volver a correr Silver.
#         Respaldos de versiones anteriores:
#         etl/backup/dag_silver_clients_quincenal_v1.3.py.bk
#         etl/backup/dag_silver_clients_quincenal_v1.4.py.bk
#
# ── Ejecución manual desde bash Docker ──────────────────
# Todas las fuentes (Gold según el interruptor, apagado por defecto):
#   airflow dags trigger dag_clients_quincenal
#
# Todas las fuentes y Gold clientes al terminar:
#   airflow dags trigger dag_clients_quincenal --conf '{"gold":true}'
#
# SOLO Gold clientes, sin volver a correr Silver
# (úsalo cuando Silver ya está al día):
#   airflow dags trigger dag_clients_quincenal --conf '{"solo_gold":true}'
#
# Una fuente específica (Gold nunca corre):
#   airflow dags trigger dag_clients_quincenal --conf '{"fuente":"fb"}'
#   airflow dags trigger dag_clients_quincenal --conf '{"fuente":"bb"}'
#   airflow dags trigger dag_clients_quincenal --conf '{"fuente":"ml"}'
#   airflow dags trigger dag_clients_quincenal --conf '{"fuente":"fi"}'
#   airflow dags trigger dag_clients_quincenal --conf '{"fuente":"vc"}'
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from airflow                        import DAG
from airflow.operators.python       import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions             import AirflowFailException
from datetime                       import datetime

from etl.etl_silver_ne              import orquestar_silver
from common.etl_silver_da           import SilverTransactionUnknownError
from common.email_notifier          import send_etl_notification

DAG_ID = 'dag_clients_quincenal'

# ── Gold clientes ────────────────────────────────────────
GOLD_CONN_ID                = 'MSSQL244'
GOLD_SQL                    = 'sql/clients/exec_sp_maestro.sql'
GOLD_HABILITADO_POR_DEFECTO = False   # ← pasar a True cuando Gold esté validado


# ════════════════════════════════════════════════════════
# Utilidad — interpretar valores de conf como booleano
# ════════════════════════════════════════════════════════

def _es_verdadero(valor) -> bool:
    """True si el valor de conf significa 'sí' (true, 1, 'si', 'yes')."""
    if isinstance(valor, str):
        return valor.strip().lower() in ('true', '1', 'si', 'sí', 'yes')
    return bool(valor)


# ════════════════════════════════════════════════════════
# Helper — wrapper común para todas las fuentes
# ════════════════════════════════════════════════════════

def _ejecutar_fuente(fuente: str, context: dict) -> None:
    """
    Ejecuta orquestar_silver para una fuente.

    Se salta (sin error) cuando:
      - se pidió solo Gold (conf solo_gold)
      - se pidió otra fuente (conf fuente)

    Comportamiento ante errores:
      False (PreWrite o Rollback) → Exception → Airflow reintenta
      SilverTransactionUnknownError → AirflowFailException
                                      → SIN reintento
    """
    conf = context.get('dag_run').conf or {}

    if _es_verdadero(conf.get('solo_gold')):
        print(f"solo_gold solicitado — saltando Silver {fuente}")
        return

    fuente_solicitada = conf.get('fuente', None)
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
# Gold — decisión y aviso de fallo
# ════════════════════════════════════════════════════════

def _debe_ejecutar_gold(**context) -> bool:
    """
    Decide si se lanza Gold clientes.
    Solo se llega aquí si las 5 tareas Silver terminaron OK
    (o se saltaron sin error por solo_gold / fuente).

    Gold SÍ se ejecuta cuando:
      - se pidió solo Gold (conf solo_gold) — decisión explícita,
        no depende del interruptor
      - las 5 fuentes corrieron, no se pidió una fuente concreta
        y el interruptor está encendido (conf gold o, si no viene,
        GOLD_HABILITADO_POR_DEFECTO)

    Gold NO se ejecuta cuando:
      - se pidió una fuente concreta (conf fuente): las otras
        cuatro tareas salen con éxito sin haber corrido, y Gold
        necesita las 5 fuentes completas
      - el interruptor está apagado
    """
    conf      = context['dag_run'].conf or {}
    fuente    = conf.get('fuente')
    solo_gold = _es_verdadero(conf.get('solo_gold'))
    gold      = conf.get('gold')

    if solo_gold:
        print("solo_gold — Silver se saltó, lanzando solo Gold clientes")
        return True

    if gold is None:
        gold = GOLD_HABILITADO_POR_DEFECTO
    else:
        gold = _es_verdadero(gold)

    if fuente:
        print(f"Se pidió solo la fuente {fuente} — Gold no se ejecuta (necesita las 5 fuentes)")
        return False

    if not gold:
        print("Gold deshabilitado — para ejecutarlo usar el disparo manual con conf gold=true o solo_gold=true")
        return False

    print("Las 5 fuentes Silver terminaron OK — lanzando Gold clientes")
    return True


def _notificar_fallo_gold(context) -> None:
    """Correo cuando falla la tarea de Gold. Un fallo aquí no altera nada."""
    try:
        send_etl_notification(
            dag_id    = DAG_ID
          , status    = 'ERROR'
          , contenido = (
                'Gold clientes (sp_etl_maestro) falló después del Silver. '
                'Revisar el log de la tarea gold_clientes y '
                'dw_etl.dw_etl_audit_log en SQL Server.'
            )
          , extra_msg = 'Gold clientes'
        )
    except Exception as e:
        print(f"WARNING: email de fallo de Gold no enviado: {e}")


# ════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════

with DAG(
    dag_id              = DAG_ID
  , description         = (
        'Clientes quincenal — Silver RAW → SQL Server '
        '(5 fuentes en paralelo, días 1 y 15, 6am) → Gold clientes (interruptor)'
    )
  , schedule_interval   = '0 6 1,15 * *'
  , start_date          = datetime(2026, 9, 19)
  , catchup             = False
  , tags                = ['clientes', 'quincenal', 'silver', 'gold']
  , params              = {'fuente': None, 'gold': None, 'solo_gold': None}
  , template_searchpath = '/opt/airflow/dags'   # ← para leer el .sql de Gold
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

    # ── Decisión: ¿se lanza Gold? ────────────────────────
    # Requiere las 5 Silver OK (regla por defecto all_success).
    # Si devuelve False, salta Gold sin marcar error.
    tarea_decidir_gold = ShortCircuitOperator(
        task_id         = 'decidir_gold'
      , python_callable = _debe_ejecutar_gold
    )

    # ── Gold clientes — mismo SP que dag_master_gold ─────
    # sp_etl_maestro: base factClientes + 5 detalles; si algún
    # detalle falla, al final lanza error y esta tarea queda en rojo.
    # Sin reintentos: lo que falló se revisa antes de repetir.
    tarea_gold = SQLExecuteQueryOperator(
        task_id             = 'gold_clientes'
      , conn_id             = GOLD_CONN_ID
      , sql                 = GOLD_SQL
      , autocommit          = True
      , retries             = 0
      , on_failure_callback = _notificar_fallo_gold
    )

    # ── Flujo ────────────────────────────────────────────
    #
    #  silver_fb ──┐
    #  silver_bb ──┤
    #  silver_ml ──┼──► decidir_gold ──► gold_clientes
    #  silver_fi ──┤     (5 OK + sin fuente + interruptor,
    #  silver_vc ──┘      o solo_gold)
    #
    #  Fallo Normal     → reintenta (retries=1)
    #  Estado Unknown   → AirflowFailException, sin reintento
    #  Si alguna Silver falla, Gold no se lanza.
    #
    [tarea_fb, tarea_bb, tarea_ml, tarea_fi, tarea_vc] >> tarea_decidir_gold >> tarea_gold
