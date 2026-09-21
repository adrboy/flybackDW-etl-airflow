# ═══════════════════════════════════════════════════════
# etl_silver_ne.py
# Capa    : Negocio (NE)
# Objetivo: Orquestar INSERT + UPDATE Silver por fuente
#           Maneja errores, logs .txt y notificación email
# Carpeta : etl/
# Versión : 1.1 — 2026-09-21
#   v1.0: fb solamente
#   v1.1: agregado bb y ml (instancia 242)
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from datetime              import datetime
from common.etl_silver_da  import (
    ejecutar_insert_silver
  , ejecutar_update_silver
  , get_log_path
)
from common.email_notifier import send_etl_notification
from common.audit_logger   import escribir_log_txt

DAG_ID = 'dag_silver_clients_quincenal'


# ════════════════════════════════════════════════════════
# Función principal — orquestar Silver por FUENTE
# Una fuente a la vez — el DAG las dispara en paralelo
# ════════════════════════════════════════════════════════

def orquestar_silver(fuente: str) -> bool:
    """
    Ejecuta INSERT + UPDATE Silver para una fuente específica.
    El DAG llama esta función en paralelo para cada fuente.

    Args:
        fuente : 'fb', 'bb', 'ml', 'fi', 'vc'

    Returns:
        True  → todo OK
        False → falló
    """
    resultados = []
    hay_error  = False

    # ── Detectar instancia por fuente ────────────────────
    instancia = '242' if fuente in ['fb', 'bb', 'ml'] else '240'
    log_path  = get_log_path(instancia)

    print(f"[{datetime.now()}] Silver {fuente} (instancia {instancia}) — iniciando")

    # ── INSERT nuevos ─────────────────────────────────────
    try:
        filas_insert, _ = ejecutar_insert_silver(fuente, DAG_ID)
        resultados.append((fuente, 'INSERT', True, filas_insert, None))
        print(f"[{datetime.now()}] {fuente} INSERT → {filas_insert:,} filas ✅")

    except Exception as e:
        hay_error = True
        msg       = str(e)
        resultados.append((fuente, 'INSERT', False, 0, msg))
        print(f"[{datetime.now()}] {fuente} INSERT → ERROR ❌ {msg}")

    # ── UPDATE modificados ────────────────────────────────
    try:
        filas_update, _, modo = ejecutar_update_silver(fuente, DAG_ID)
        resultados.append((fuente, f'UPDATE-{modo.upper()}', True, filas_update, None))
        print(f"[{datetime.now()}] {fuente} UPDATE({modo}) → {filas_update:,} filas ✅")

    except Exception as e:
        hay_error = True
        msg       = str(e)
        resultados.append((fuente, 'UPDATE', False, 0, msg))
        print(f"[{datetime.now()}] {fuente} UPDATE → ERROR ❌ {msg}")

    # ── Generar reporte .txt ──────────────────────────────
    estado_general = 'OK' if not hay_error else 'ERROR'
    reporte        = _generar_reporte(fuente, instancia, resultados, estado_general)

    try:
        escribir_log_txt(
            log_path  = log_path
          , vista     = f'silver_clients_{fuente}'
          , reporte   = reporte
          , dag_id    = DAG_ID
          , estado    = estado_general
          , notificar = False
        )
    except Exception as e:
        print(f"[{datetime.now()}] WARNING: log .txt falló: {e}")

    # ── Notificación email ────────────────────────────────
    try:
        send_etl_notification(
            dag_id    = DAG_ID
          , status    = estado_general
          , log_path  = log_path
          , extra_msg = f"Silver {fuente.upper()} (instancia {instancia})"
        )
    except Exception as e:
        print(f"[{datetime.now()}] WARNING: email falló: {e}")

    return not hay_error


# ════════════════════════════════════════════════════════
# Generador de reporte — uso interno
# ════════════════════════════════════════════════════════

def _generar_reporte(fuente: str, instancia: str, resultados: list, estado: str) -> str:
    """Genera reporte .txt con el resultado de cada operación."""
    ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sep   = '═' * 52
    emoji = '✅' if estado == 'OK' else '❌'

    lineas = [
        sep
      , f'  ETL SILVER QUINCENAL — {DAG_ID}'
      , sep
      , f'Fecha     : {ahora}'
      , f'Fuente    : {fuente.upper()} (instancia {instancia})'
      , f'RESULTADO : {emoji} {estado}'
      , sep
      , ''
      , '══ DETALLE POR OPERACIÓN ' + '═' * 27
    ]

    for f, operacion, ok, filas, mensaje in resultados:
        if ok:
            lineas.append(f'  ✅ {f.upper()} {operacion} → {filas:,} filas')
        else:
            lineas.append(f'  ❌ {f.upper()} {operacion} → ERROR')
            lineas.append(f'     {mensaje[:200]}')

    lineas.append('')
    lineas.append(sep)
    return '\n'.join(lineas)
