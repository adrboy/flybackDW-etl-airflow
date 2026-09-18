# ═══════════════════════════════════════════════════════
# etl_raw_ne.py
# Capa    : Negocio (NE)
# Objetivo: Orquestar la ejecución de SPs CDC por instancia
#           Maneja errores, logs .txt y notificación email
# Carpeta : etl/
# Versión : 1.0 — 2026-09-17
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from datetime              import datetime
from common.etl_raw_da     import (
    get_conexion
  , get_procedimientos
  , get_log_config
  , ejecutar_sp
  , registrar_log
)
from common.email_notifier import send_etl_notification
from common.audit_logger   import escribir_log_txt

DAG_ID = 'dag_raw_clients_cdc_diario'


# ════════════════════════════════════════════════════════
# Función principal — orquestar por instancia
# ════════════════════════════════════════════════════════

def orquestar(instancia: str) -> bool:
    """
    Ejecuta todos los SPs CDC de la instancia indicada.
    Maneja errores, registra log en BD, escribe .txt
    y notifica por email.

    Args:
        instancia : '242' o '240'

    Returns:
        True  → todos los SPs ejecutaron OK
        False → al menos uno falló
    """
    conn_id      = get_conexion(instancia)
    procedimientos = get_procedimientos(instancia)
    log_cfg      = get_log_config(instancia)

    resultados   = []   # [(sp, True/False, mensaje)]
    hay_error    = False

    print(f"[{datetime.now()}] Instancia {instancia} — {len(procedimientos)} SPs a ejecutar")

    # ── Ejecutar cada SP ─────────────────────────────────
    for sp in procedimientos:
        fecha_inicio = datetime.now()
        try:
            # ── Capa datos: ejecutar ──────────────────────
            ejecutar_sp(conn_id, sp)

            # ── Capa datos: registrar OK en BD ────────────
            registrar_log(
                instancia    = instancia
              , paquete      = DAG_ID
              , sp           = sp
              , estado       = 'OK'
              , fecha_inicio = fecha_inicio
            )
            resultados.append((sp, True, None))
            print(f"[{datetime.now()}] {sp} — registrado OK ✅")

        except Exception as e:
            hay_error     = True
            msg_error     = str(e)

            # ── Capa datos: registrar ERROR en BD ─────────
            try:
                registrar_log(
                    instancia     = instancia
                  , paquete       = DAG_ID
                  , sp            = sp
                  , estado        = 'ERROR'
                  , mensaje_error = msg_error
                  , fecha_inicio  = fecha_inicio
                )
            except Exception as log_err:
                print(f"[{datetime.now()}] WARNING: No se pudo registrar error en BD: {log_err}")

            resultados.append((sp, False, msg_error))
            print(f"[{datetime.now()}] {sp} — ERROR ❌ {msg_error}")

    # ── Generar reporte .txt ──────────────────────────────
    estado_general = 'OK' if not hay_error else 'ERROR'
    reporte        = _generar_reporte(instancia, resultados, estado_general)

    try:
        log_path = escribir_log_txt(
            log_path  = log_cfg['path']
          , vista     = f'raw_clients_{instancia}'
          , reporte   = reporte
          , dag_id    = DAG_ID
          , estado    = estado_general
          , notificar = False   # ← email lo manejamos aquí
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
          , extra_msg = f"Instancia {instancia} — {len(procedimientos)} SPs procesados"
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
        resultados : [(sp, ok, mensaje)]
        estado     : 'OK' o 'ERROR'

    Returns:
        String con el reporte completo
    """
    ahora  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sep    = '═' * 52
    emoji  = '✅' if estado == 'OK' else '❌'

    lineas = [
        sep
      , f'  ETL RAW CDC — {DAG_ID}'
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
