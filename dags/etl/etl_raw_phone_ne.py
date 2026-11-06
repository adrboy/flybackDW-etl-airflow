# ═══════════════════════════════════════════════════════
# etl_raw_phone_ne.py
# Capa    : Negocio (NE)
# Objetivo: Orquestar la ejecución de los SPs RAW de teléfonos
#           por instancia. Maneja errores, auditoría en BD,
#           log .txt y notificación email
# Carpeta : etl/
# Versión : 1.1 — BORRADOR pendiente de pruebas desde Airflow
#   v1.0: versión inicial (correo en cada corrida)
#   v1.1: DAG_ID → dag_raw_phones_diario; el correo solo se envía
#         cuando algún SP falla o es bloqueado (NOTIFICAR_SOLO_ERROR)
# Nota    : Un SP que falla o que la compuerta bloquea NO frena
#           a los demás. Si falla el registro de auditoría solo
#           se avisa en el log: no cambia el resultado del SP.
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from datetime                import datetime
from common.etl_raw_phone_da import (
    get_conexion
  , get_procedimientos
  , get_log_config
  , get_max_pct
  , ejecutar_sp
  , registrar_log
)
from common.email_notifier   import send_etl_notification
from common.audit_logger     import escribir_log_txt

DAG_ID = 'dag_raw_phones_diario'

# True  → el correo solo sale si algún SP falla o es bloqueado
# False → el correo sale en cada corrida (también cuando todo va bien)
NOTIFICAR_SOLO_ERROR = True


# ════════════════════════════════════════════════════════
# Función principal — orquestar por instancia
# ════════════════════════════════════════════════════════

def orquestar(instancia: str) -> bool:
    """
    Ejecuta todos los SPs RAW de teléfonos de la instancia indicada.

    Args:
        instancia : '242' o '240'

    Returns:
        True  → todos los SPs ejecutaron OK
        False → al menos uno falló o fue bloqueado por la compuerta
    """
    conn_id        = get_conexion(instancia)
    procedimientos = get_procedimientos(instancia)
    log_cfg        = get_log_config(instancia)

    resultados = []   # [(sp, estado, detalle)]  detalle = dict (OK) o texto (error)
    hay_error  = False

    print(f"[{datetime.now()}] Instancia {instancia} — {len(procedimientos)} SPs a ejecutar")

    for sp in procedimientos:
        fecha_inicio = datetime.now()
        filas        = None
        msg_error    = None

        # ── Ejecutar el SP ────────────────────────────────
        try:
            res    = ejecutar_sp(conn_id, sp, get_max_pct(sp), 0)
            estado = 'OK'
            filas  = res.get('nuevos')
            resultados.append((sp, estado, res))
            print(f"[{datetime.now()}] {sp} — OK ✅")

        except Exception as e:
            hay_error = True
            msg_error = str(e)
            estado    = 'BLOQUEADO' if 'BLOQUEADO' in msg_error else 'ERROR'
            resultados.append((sp, estado, msg_error))
            print(f"[{datetime.now()}] {sp} — {estado} ❌ {msg_error}")

        # ── Registrar en auditoría (separado del SP) ─────
        try:
            registrar_log(
                instancia        = instancia
              , paquete          = DAG_ID
              , sp               = sp
              , estado           = estado
              , filas_insertadas = filas
              , mensaje_error    = msg_error
              , fecha_inicio     = fecha_inicio
            )
        except Exception as log_err:
            print(f"[{datetime.now()}] WARNING: No se pudo registrar auditoría de {sp}: {log_err}")

    # ── Generar reporte .txt ──────────────────────────────
    estado_general = 'OK' if not hay_error else 'ERROR'
    reporte        = _generar_reporte(instancia, resultados, estado_general)

    try:
        log_path = escribir_log_txt(
            log_path  = log_cfg['path']
          , vista     = f'raw_phones_{instancia}'
          , reporte   = reporte
          , dag_id    = DAG_ID
          , estado    = estado_general
          , notificar = False   # ← email lo manejamos aquí
        )
    except Exception as txt_err:
        print(f"[{datetime.now()}] WARNING: No se pudo escribir log .txt: {txt_err}")
        log_path = None

    # ── Notificación email ────────────────────────────────
    if hay_error or not NOTIFICAR_SOLO_ERROR:
        try:
            send_etl_notification(
                dag_id    = DAG_ID
              , status    = estado_general
              , log_path  = log_path
              , extra_msg = f"Instancia {instancia} — {len(procedimientos)} SPs de teléfonos procesados"
            )
        except Exception as email_err:
            print(f"[{datetime.now()}] WARNING: No se pudo enviar email: {email_err}")

    return not hay_error


# ════════════════════════════════════════════════════════
# Generador de reporte — uso interno
# ════════════════════════════════════════════════════════

def _generar_reporte(instancia: str, resultados: list, estado: str) -> str:
    """
    Genera reporte .txt con el resultado de cada SP.

    Args:
        instancia  : '242' o '240'
        resultados : [(sp, estado, detalle)]
        estado     : 'OK' o 'ERROR'

    Returns:
        String con el reporte completo
    """
    ahora  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sep    = '═' * 52
    emoji  = '✅' if estado == 'OK' else '❌'

    lineas = [
        sep
      , f'  ETL RAW TELÉFONOS — {DAG_ID}'
      , sep
      , f'Fecha     : {ahora}'
      , f'Instancia : {instancia}'
      , f'RESULTADO : {emoji} {estado}'
      , sep
      , ''
      , '══ DETALLE POR SP ' + '═' * 34
    ]

    for sp, est, detalle in resultados:
        nombre = sp.split('.')[-1]
        if est == 'OK':
            lineas.append(f'  ✅ {nombre}')
            lineas.append(
                f"     nuevos={detalle.get('nuevos')}"
                f"  revividos={detalle.get('revividos')}"
                f"  marcados={detalle.get('marcados')}"
                f"  vivos_final={detalle.get('vivos_final')}"
                f"  segundos={detalle.get('segundos')}"
            )
        else:
            lineas.append(f'  ❌ {nombre} — {est}')
            lineas.append(f'     {str(detalle)[:200]}')

    lineas.append('')
    lineas.append(sep)

    return '\n'.join(lineas)
