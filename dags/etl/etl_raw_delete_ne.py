# ═══════════════════════════════════════════════════════
# etl_raw_delete_ne.py
# Capa    : Negocio (NE)
# Objetivo: Orquestar la ejecución de SPs DELETE por instancia
#           Maneja errores, logs .txt y notificación email
# Carpeta : etl/
# Versión : 1.0 — 2026-09-18
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from datetime                  import datetime
from common.etl_raw_delete_da  import (
    get_conexion
  , get_procedimientos
  , get_log_config
  , ejecutar_sp
)
from common.email_notifier     import send_etl_notification
from common.audit_logger       import escribir_log_txt

DAG_ID = 'dag_raw_clients_delete_mensual'


# ════════════════════════════════════════════════════════
# Función principal — orquestar DELETE por instancia
# ════════════════════════════════════════════════════════

def orquestar(instancia: str) -> bool:
    """
    Ejecuta todos los SPs DELETE de la instancia indicada.
    Maneja errores, registra log en BD, escribe .txt
    y notifica por email.

    Args:
        instancia : '242' o '240'

    Returns:
        True  → todos los SPs ejecutaron OK
        False → al menos uno falló
    """
    conn_id        = get_conexion(instancia)
    procedimientos = get_procedimientos(instancia)
    log_cfg        = get_log_config(instancia)

    resultados  = []   # [(sp, True/False, mensaje)]
    hay_error   = False

    print(f"[{datetime.now()}] DELETE instancia {instancia} — {len(procedimientos)} SPs")

    # ── Ejecutar cada SP DELETE ──────────────────────────
    for sp in procedimientos:
        try:
            ejecutar_sp(conn_id, sp)
            resultados.append((sp, True, None))
            print(f"[{datetime.now()}] {sp} — OK ✅")

        except Exception as e:
            hay_error = True
            msg_error = str(e)
            resultados.append((sp, False, msg_error))
            print(f"[{datetime.now()}] {sp} — ERROR ❌ {msg_error}")

    # ── Generar reporte .txt ──────────────────────────────
    estado_general = 'OK' if not hay_error else 'ERROR'
    reporte        = _generar_reporte(instancia, resultados, estado_general)

    try:
        log_path = escribir_log_txt(
            log_path  = log_cfg['path']
          , vista     = f'raw_delete_{instancia}'
          , reporte   = reporte
          , dag_id    = DAG_ID
          , estado    = estado_general
          , notificar = False
        )
    except Exception as txt_err:
        print(f"[{datetime.now()}] WARNING: No se pudo escribir log .txt: {txt_err}")
        log_path = None

    # ── Notificación email ────────────────────────────────
    try:
        send_etl_notification(
            dag_id    = DAG_ID
          , status    = estado_general
          , log_path  = log_path
          , extra_msg = f"Instancia {instancia} — VERIFY-DELETE mensual"
        )
    except Exception as email_err:
        print(f"[{datetime.now()}] WARNING: No se pudo enviar email: {email_err}")

    return not hay_error


# ════════════════════════════════════════════════════════
# Generador de reporte — uso interno
# ════════════════════════════════════════════════════════

def _generar_reporte(instancia: str, resultados: list, estado: str) -> str:
    """
    Genera reporte .txt con el resultado de cada SP DELETE.
    """
    ahora  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sep    = '═' * 52
    emoji  = '✅' if estado == 'OK' else '❌'

    lineas = [
        sep
      , f'  ETL RAW DELETE MENSUAL — {DAG_ID}'
      , sep
      , f'Fecha     : {ahora}'
      , f'Instancia : {instancia}'
      , f'RESULTADO : {emoji} {estado}'
      , sep
      , ''
      , '══ DETALLE POR SP ' + '═' * 34
    ]

    for sp, ok, mensaje in resultados:
        nombre = sp.split('.')[-1]
        if ok:
            lineas.append(f'  ✅ {nombre}')
        else:
            lineas.append(f'  ❌ {nombre}')
            lineas.append(f'     ERROR: {mensaje[:200]}')

    lineas.append('')
    lineas.append(sep)

    return '\n'.join(lineas)
