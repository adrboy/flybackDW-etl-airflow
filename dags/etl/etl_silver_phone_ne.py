# ═══════════════════════════════════════════════════════
# etl_silver_phone_ne.py
# Capa    : Negocio (NE)
# Objetivo: Orquestar Silver de teléfonos por fuente
#           Maneja errores, logs .txt y notificación email
# Carpeta : etl/
# Versión : 1.0 — 2026-11-09 — BORRADOR pendiente de pruebas
# Molde   : etl_silver_ne.py v2.2 — mismo contrato de retorno
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from datetime                   import datetime
from common.etl_silver_phone_da import (
    ejecutar_silver_phone
  , get_log_path
)
from common.etl_silver_da       import (
    SilverPreWriteError
  , SilverRollbackError
  , SilverTransactionUnknownError
)
from common.email_notifier      import send_etl_notification
from common.audit_logger        import escribir_log_txt

DAG_ID = 'dag_phones_quincenal'

# True  → el correo solo sale si Silver falla
# False → el correo sale en cada corrida (también cuando todo va bien)
NOTIFICAR_SOLO_ERROR = False


# ════════════════════════════════════════════════════════
# Función principal — orquestar Silver de teléfonos por FUENTE
# ════════════════════════════════════════════════════════

def orquestar_silver_phone(fuente: str) -> bool:
    """
    Ejecuta Silver incremental de teléfonos (UPDATE + INSERT en una
    sola transacción, con candado y auditoría).

    Contrato de retorno:
      True  → COMMIT confirmado ✅
      False → Silver intacto (PreWrite o Rollback confirmado)
              → DAG reintenta (retries=1)

    Contrato de excepción:
      SilverTransactionUnknownError → notifica + re-lanza
              → DAG captura con AirflowFailException
              → SIN reintento

    Args:
        fuente : 'fb', 'bb', 'ml', 'fi', 'vc'
    """
    instancia = '242' if fuente in ['fb', 'bb', 'ml'] else '240'
    log_path  = get_log_path(instancia)

    print(f"[{datetime.now()}] Silver teléfonos {fuente} (instancia {instancia}) — iniciando")

    # ════════════════════════════════════════════════════
    # BLOQUE 1 — Transacción de datos (DA)
    # ════════════════════════════════════════════════════
    try:
        filas_update, filas_insert, filas_leidas, segundos, marca = \
            ejecutar_silver_phone(fuente)

    except SilverPreWriteError as e:
        print(f"[{datetime.now()}] {fuente} — Error pre-escritura ❌ {e}")
        _notificar(
            fuente    = fuente
          , instancia = instancia
          , log_path  = log_path
          , estado    = 'ERROR'
          , lineas    = [
                f'  ❌ {fuente.upper()} — Error antes de escribir en Silver'
              , f'     Silver sin cambios — el próximo run reintenta'
              , f'     {str(e)[:200]}'
            ]
        )
        return False

    except SilverRollbackError as e:
        print(f"[{datetime.now()}] {fuente} — ROLLBACK confirmado ❌ {e}")
        _notificar(
            fuente    = fuente
          , instancia = instancia
          , log_path  = log_path
          , estado    = 'ERROR'
          , lineas    = [
                f'  ❌ {fuente.upper()} — ROLLBACK confirmado'
              , f'     Silver sin cambios — el próximo run reintenta limpio'
              , f'     {str(e)[:200]}'
            ]
        )
        return False

    except SilverTransactionUnknownError as e:
        print(f"[{datetime.now()}] {fuente} — Estado desconocido ⚠️ {e}")
        _notificar(
            fuente    = fuente
          , instancia = instancia
          , log_path  = log_path
          , estado    = 'ERROR'
          , lineas    = [
                f'  ⚠️  {fuente.upper()} — Estado de Silver desconocido'
              , f'     Revisar manualmente antes del próximo run'
              , f'     NO se reintentará automáticamente'
              , f'     {str(e)[:200]}'
            ]
        )
        raise   # ← DAG captura con AirflowFailException

    # ════════════════════════════════════════════════════
    # BLOQUE 2 — Reporte post-COMMIT
    # COMMIT confirmado — datos en Silver seguros ✅
    # ════════════════════════════════════════════════════
    _notificar(
        fuente    = fuente
      , instancia = instancia
      , log_path  = log_path
      , estado    = 'OK'
      , lineas    = [
            f'  ✅ {fuente.upper()} RAW → Silver — leídas desde {marca}: {filas_leidas:,}'
          , f'  ✅ {fuente.upper()} UPDATE → {filas_update:,} filas'
          , f'  ✅ {fuente.upper()} INSERT → {filas_insert:,} filas'
          , f'     Tiempo: {segundos:.1f}s — COMMIT confirmado'
        ]
    )
    return True


# ════════════════════════════════════════════════════════
# Notificación — log .txt + email
# TXT y email son completamente independientes
# ════════════════════════════════════════════════════════

def _notificar(
    fuente    : str
  , instancia : str
  , log_path  : str
  , estado    : str
  , lineas    : list
) -> None:
    """
    Genera reporte, escribe TXT y envía email (según NOTIFICAR_SOLO_ERROR).

    Garantías:
      1. reporte generado en memoria con fallback mínimo
      2. TXT escrito desde memoria → si falla: WARNING
      3. Email recibe el reporte desde memoria (contenido=)
      TXT y email son independientes entre sí.
      Ningún fallo aquí altera el estado real de Silver.
    """

    # ── PASO 1: Generar reporte en memoria ────────────────
    try:
        reporte = _generar_reporte(fuente, instancia, lineas, estado)
    except Exception as e:
        print(f"[{datetime.now()}] WARNING: _generar_reporte falló: {e}")
        reporte = (
            f"Silver teléfonos {fuente.upper()} (instancia {instancia}) — "
            f"{estado} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"[Reporte no disponible: {e}]"
        )

    # ── PASO 2: Escribir TXT desde memoria ───────────────
    try:
        escribir_log_txt(
            log_path  = log_path
          , vista     = f'silver_phones_{fuente}'
          , reporte   = reporte
          , dag_id    = DAG_ID
          , estado    = estado
          , notificar = False
        )
    except Exception as e:
        print(f"[{datetime.now()}] WARNING: TXT no guardado: {e}")

    # ── PASO 3: Enviar email desde memoria ────────────────
    if estado == 'OK' and NOTIFICAR_SOLO_ERROR:
        return

    try:
        enviado = send_etl_notification(
            dag_id    = DAG_ID
          , status    = estado
          , contenido = reporte
          , extra_msg = f"Silver teléfonos {fuente.upper()} (instancia {instancia})"
        )
        if not enviado:
            print(f"[{datetime.now()}] WARNING: email no enviado ({fuente})")
    except Exception as e:
        print(f"[{datetime.now()}] WARNING: email falló: {e}")


# ════════════════════════════════════════════════════════
# Generador de reporte — uso interno
# ════════════════════════════════════════════════════════

def _generar_reporte(
    fuente    : str
  , instancia : str
  , lineas    : list
  , estado    : str
) -> str:
    """Genera reporte .txt con el resultado de la operación."""
    ahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    sep   = '═' * 52
    emoji = '✅' if estado == 'OK' else '❌'

    resultado = [
        sep
      , f'  ETL SILVER TELÉFONOS — {DAG_ID}'
      , sep
      , f'Fecha     : {ahora}'
      , f'Fuente    : {fuente.upper()} (instancia {instancia})'
      , f'RESULTADO : {emoji} {estado}'
      , sep
      , ''
      , '══ DETALLE ' + '═' * 41
    ]

    resultado.extend(lineas)
    resultado.append('')
    resultado.append(sep)
    return '\n'.join(resultado)
