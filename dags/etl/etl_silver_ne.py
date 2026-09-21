# ═══════════════════════════════════════════════════════
# etl_silver_ne.py
# Capa    : Negocio (NE)
# Objetivo: Orquestar INSERT + UPDATE Silver por fuente
#           Maneja errores, logs .txt y notificación email
# Carpeta : etl/
# Versión : 1.0 — 2026-09-19
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

# ── Fuentes por instancia ────────────────────────────────
_FUENTES = {
    '242': ['fb']          # ← bb, ml se agregan aquí después
  , '240': []              # ← fi, vc se agregan aquí después
}


# ════════════════════════════════════════════════════════
# Función principal — orquestar Silver por instancia
# ════════════════════════════════════════════════════════

def orquestar_silver(instancia: str) -> bool:
    """
    Ejecuta INSERT + UPDATE Silver para todas las fuentes
    de la instancia indicada.

    Args:
        instancia : '242' o '240'

    Returns:
        True  → todo OK
        False → al menos una fuente falló
    """
    fuentes    = _FUENTES[instancia]
    resultados = []
    hay_error  = False
    log_path   = get_log_path(instancia)

    print(f"[{datetime.now()}] Silver instancia {instancia} — {len(fuentes)} fuentes")

    for fuente in fuentes:

        # ── INSERT nuevos ─────────────────────────────────
        try:
            filas_insert, reporte_insert = ejecutar_insert_silver(fuente, DAG_ID)
            resultados.append((fuente, 'INSERT', True, filas_insert, None))
            print(f"[{datetime.now()}] {fuente} INSERT → {filas_insert:,} filas ✅")

        except Exception as e:
            hay_error = True
            msg       = str(e)
            resultados.append((fuente, 'INSERT', False, 0, msg))
            print(f"[{datetime.now()}] {fuente} INSERT → ERROR ❌ {msg}")

        # ── UPDATE modificados ────────────────────────────
        try:
            filas_update, reporte_update, modo = ejecutar_update_silver(fuente, DAG_ID)
            resultados.append((fuente, f'UPDATE-{modo.upper()}', True, filas_update, None))
            print(f"[{datetime.now()}] {fuente} UPDATE({modo}) → {filas_update:,} filas ✅")

        except Exception as e:
            hay_error = True
            msg       = str(e)
            resultados.append((fuente, 'UPDATE', False, 0, msg))
            print(f"[{datetime.now()}] {fuente} UPDATE → ERROR ❌ {msg}")

    # ── Generar reporte .txt ──────────────────────────────
    estado_general = 'OK' if not hay_error else 'ERROR'
    reporte        = _generar_reporte(instancia, resultados, estado_general)

    try:
        escribir_log_txt(
            log_path  = log_path
          , vista     = f'silver_clients_{instancia}'
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
          , extra_msg = f"Instancia {instancia} — Silver quincenal"
        )
    except Exception as e:
        print(f"[{datetime.now()}] WARNING: email falló: {e}")

    return not hay_error


# ════════════════════════════════════════════════════════
# Generador de reporte — uso interno
# ════════════════════════════════════════════════════════

def _generar_reporte(instancia: str, resultados: list, estado: str) -> str:
    """Genera reporte .txt con el resultado de cada operación."""
    ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sep   = '═' * 52
    emoji = '✅' if estado == 'OK' else '❌'

    lineas = [
        sep
      , f'  ETL SILVER QUINCENAL — {DAG_ID}'
      , sep
      , f'Fecha     : {ahora}'
      , f'Instancia : {instancia}'
      , f'RESULTADO : {emoji} {estado}'
      , sep
      , ''
      , '══ DETALLE POR OPERACIÓN ' + '═' * 27
    ]

    for fuente, operacion, ok, filas, mensaje in resultados:
        if ok:
            lineas.append(f'  ✅ {fuente} {operacion} → {filas:,} filas')
        else:
            lineas.append(f'  ❌ {fuente} {operacion} → ERROR')
            lineas.append(f'     {mensaje[:200]}')

    lineas.append('')
    lineas.append(sep)
    return '\n'.join(lineas)
