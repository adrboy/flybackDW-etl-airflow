# ═══════════════════════════════════════════════════════
# dag_phones_quincenal.py
# Capa    : Negocio / Orquestador (DAG)
# Objetivo: Teléfonos quincenal
#           1) Silver: RAW MariaDB (tbl_raw_phoneXX) → source.PhoneXX
#              incremental, UPDATE + INSERT por conjuntos (5 fuentes en cadena)
#           2) Gold: al terminar las 5 OK → sp_insert_phones_factPersonalInfo v1.2
#              (MERGE sobre gral.factPersonalInfo) detrás de un interruptor
#              ENCENDIDO por defecto desde el 2026-11-12
# Carpeta : etl/
# Versión : 1.1 — 2026-11-12
#   v1.0: Silver de las cinco fuentes (probadas una por una el 2026-11-09)
#   v1.1: tarea final Gold: EXEC [dw_etl].[sp_insert_phones_factPersonalInfo]
#         (conexión MSSQL244, igual que dag_clients_quincenal).
#         Solo corre si: (1) las 5 fuentes Silver terminaron OK,
#                        (2) no se pidió una fuente concreta,
#                        (3) el interruptor gold está encendido.
#         Interruptor: GOLD_HABILITADO_POR_DEFECTO (False hasta el 2026-11-12; hoy True)
#         → con False, en las corridas programadas Gold se SALTA (hoy: True, corre).
#         Opción solo_gold: ejecuta únicamente Gold, sin volver a correr Silver.
#         El SP v1.2 trae su propia compuerta de retiro (@max_pct = 2%) y
#         candado; sin parámetros procesa los 5 productos.
#
# ── Orden de ejecución (en cadena) ──────────────────────
#   242 primero: fb → bb → ml     después 240: fi → vc
#   Si una tarea falla, las siguientes SIGUEN (trigger_rule all_done).
#   decidir_gold depende de TODAS las fuentes (no solo de la última).
#
# ── Requisito de Gold ───────────────────────────────────
#   gral.factClientesDetalle al día (Gold de clientes primero): el idcorreo
#   de cada teléfono sale de ahí. Los teléfonos de clientes que Gold todavía
#   no conoce se insertan en la corrida siguiente, cuando ya estén mapeados.
#
# ── Ejecución manual desde bash Docker ──────────────────
# Todas las fuentes (Gold según el interruptor, encendido por defecto):
#   airflow dags trigger dag_phones_quincenal
#
# Todas las fuentes y Gold teléfonos al terminar:
#   airflow dags trigger dag_phones_quincenal --conf '{"gold":true}'
#
# SOLO Gold teléfonos, sin volver a correr Silver
# (úsalo cuando Silver ya está al día):
#   airflow dags trigger dag_phones_quincenal --conf '{"solo_gold":true}'
#
# Una fuente específica (Gold nunca corre):
#   airflow dags trigger dag_phones_quincenal --conf '{"fuente":"fb"}'
#
# Nota: se crea PAUSADO. Al activarlo, Airflow puede lanzar de
#       inmediato una corrida por el intervalo ya vencido
#       (comportamiento normal con catchup=False).
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from airflow                  import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.exceptions       import AirflowFailException
from datetime                 import datetime

from etl.etl_silver_phone_ne  import orquestar_silver_phone
from common.etl_silver_da     import SilverTransactionUnknownError
from common.email_notifier    import send_etl_notification

DAG_ID = 'dag_phones_quincenal'

# Orden de la cadena: 242 primero (fb, bb, ml) y después 240 (fi, vc).
FUENTES = [
    'fb'
  , 'bb'
  , 'ml'
  , 'fi'
  , 'vc'
]

# ── Gold teléfonos ───────────────────────────────────────
GOLD_CONN_ID                = 'MSSQL244'
GOLD_SQL                    = 'sql/clients/exec_sp_phones.sql'
GOLD_HABILITADO_POR_DEFECTO = True    # ← ENCENDIDO el 2026-11-12: Gold corre solo al terminar Silver


# ════════════════════════════════════════════════════════
# Utilidad — interpretar valores de conf como booleano
# ════════════════════════════════════════════════════════

def _es_verdadero(valor) -> bool:
    """True si el valor de conf significa 'sí' (true, 1, 'si', 'yes')."""
    if isinstance(valor, str):
        return valor.strip().lower() in ('true', '1', 'si', 'sí', 'yes')
    return bool(valor)


# ════════════════════════════════════════════════════════
# Wrapper común — mismo contrato que dag_clients_quincenal
# ════════════════════════════════════════════════════════

def _tarea_silver_phone(fuente: str, **context) -> None:
    """
    Ejecuta Silver de teléfonos para una fuente.

    Se salta (sin error) cuando:
      - se pidió solo Gold (conf solo_gold)
      - se pidió otra fuente (conf fuente)

    Comportamiento ante errores:
      False (PreWrite o Rollback) → Exception → Airflow reintenta
      SilverTransactionUnknownError → AirflowFailException
                                      → SIN reintento
    """
    dag_run = context.get('dag_run')
    conf = (dag_run.conf if dag_run else None) or {}

    if _es_verdadero(conf.get('solo_gold')):
        print(f"solo_gold solicitado — saltando Silver {fuente}")
        return

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
# Gold — decisión y aviso de fallo
# ════════════════════════════════════════════════════════

def _debe_ejecutar_gold(**context) -> bool:
    """
    Decide si se lanza Gold teléfonos.
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
        tareas salen con éxito sin haber corrido, y Gold
        necesita las 5 fuentes completas
      - el interruptor está apagado
    """
    dag_run   = context.get('dag_run')
    conf      = (dag_run.conf if dag_run else None) or {}
    fuente    = conf.get('fuente')
    solo_gold = _es_verdadero(conf.get('solo_gold'))
    gold      = conf.get('gold')

    if solo_gold:
        print("solo_gold — Silver se saltó, lanzando solo Gold teléfonos")
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

    print("Las 5 fuentes Silver terminaron OK — lanzando Gold teléfonos")
    return True


def _notificar_fallo_gold(context) -> None:
    """Correo cuando falla la tarea de Gold. Un fallo aquí no altera nada."""
    try:
        send_etl_notification(
            dag_id    = DAG_ID
          , status    = 'ERROR'
          , contenido = (
                'Gold teléfonos (sp_insert_phones_factPersonalInfo) falló después del Silver. '
                'Revisar el log de la tarea gold_phones y '
                'dw_etl.dw_etl_audit_log en SQL Server. '
                'Si el estado es Blocked, la compuerta de retiro se detuvo sin escribir nada.'
            )
          , extra_msg = 'Gold teléfonos'
        )
    except Exception as e:
        print(f"WARNING: email de fallo de Gold no enviado: {e}")


# ════════════════════════════════════════════════════════
# DAG
# ════════════════════════════════════════════════════════

with DAG(
    dag_id                    = DAG_ID
  , description               = (
        'Teléfonos quincenal — Silver RAW → SQL Server '
        '(cadena fb/bb/ml/fi/vc, días 1 y 15, 7am) → Gold teléfonos (interruptor)'
    )
  , schedule_interval         = '0 7 1,15 * *'
  , start_date                = datetime(2026, 11, 1)
  , catchup                   = False
  , max_active_runs           = 1
  , is_paused_upon_creation   = True
  , tags                      = ['telefonos', 'quincenal', 'silver', 'gold']
  , params                    = {'fuente': None, 'gold': None, 'solo_gold': None}
  , template_searchpath       = '/opt/airflow/dags'   # ← para leer el .sql de Gold
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

    # ── Decisión: ¿se lanza Gold? ────────────────────────
    # Depende de LAS CINCO tareas Silver (no solo de la última de la cadena:
    # con all_done la última puede terminar bien aunque una anterior haya
    # fallado). Regla por defecto all_success. Si devuelve False, salta Gold
    # sin marcar error.
    tarea_decidir_gold = ShortCircuitOperator(
        task_id         = 'decidir_gold'
      , python_callable = _debe_ejecutar_gold
    )

    # ── Gold teléfonos — SP v1.2 (MERGE), sin parámetros = los 5 ──
    # La compuerta de retiro y el candado viven dentro del SP.
    # Sin reintentos: lo que falló se revisa antes de repetir.
    # do_xcom_push=False: el SP devuelve un resultado (insertados / revividos /
    # retirados) que no hace falta guardar; así no puede fallar la tarea por
    # serializar una fila después de que el SP ya confirmó.
    tarea_gold = SQLExecuteQueryOperator(
        task_id             = 'gold_phones'
      , conn_id             = GOLD_CONN_ID
      , sql                 = GOLD_SQL
      , autocommit          = True
      , retries             = 0
      , do_xcom_push        = False
      , on_failure_callback = _notificar_fallo_gold
    )

    # ── Flujo ────────────────────────────────────────────
    #
    #  silver_phone_fb ─► silver_phone_bb ─► silver_phone_ml
    #                                           ─► silver_phone_fi ─► silver_phone_vc
    #  (las cinco)  ──►  decidir_gold  ──►  gold_phones
    #                    (5 OK + sin fuente + interruptor, o solo_gold)
    #
    #  Fallo Normal     → reintenta (retries=1)
    #  Estado Unknown   → AirflowFailException, sin reintento
    #  Una tarea fallida NO detiene a las siguientes Silver,
    #  pero sí impide que Gold se lance.
    #
    tareas >> tarea_decidir_gold >> tarea_gold
