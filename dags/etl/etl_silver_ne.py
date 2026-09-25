# ═══════════════════════════════════════════════════════
# etl_silver_ne.py
# Capa    : Negocio (NE)
# Objetivo: Orquestar Silver por fuente
#           Maneja errores, logs .txt y notificación email
# Carpeta : etl/
# Versión : 2.1 — 2026-09-24
#   v2.0: _notificar corregida — ruta_reporte al email
#   v2.1: reporte en memoria → send_etl_notification(contenido=)
#         TXT y email completamente independientes
#         ninguno depende del otro para tener el reporte
# ═══════════════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')

from datetime              import datetime
from common.etl_silver_da  import (
    ejecutar_silver
  , get_log_path
  , SilverPreWriteError
  , SilverRollbackError
  , SilverTransactionUnknownError
)
from common.email_notifier import send_etl_notification
from common.audit_logger   import escribir_log_txt

DAG_ID = 'dag_silver_clients_quincenal'


# ════════════════════════════════════════════════════════
# Función principal — orquestar Silver por FUENTE
# ════════════════════════════════════════════════════════

def orquestar_silver(fuente: str) -> bool:
    """
    Ejecuta UPDATE + INSERT Silver en una sola transacción.

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

    print(f"[{datetime.now()}] Silver {fuente} (instancia {instancia}) — iniciando")

    # ════════════════════════════════════════════════════
    # BLOQUE 1 — Transacción de datos (DA)
    # ════════════════════════════════════════════════════
    try:
        filas_update, filas_insert, modo, segundos = ejecutar_silver(fuente)

    except SilverPreWriteError as e:
        print(f"[{datetime.now()}] {fuente} — Error pre-escritura ❌ {e}")
        _notificar(
            fuente    = fuente
          , instancia = instancia
          , log_path  = log_path
          , estado    = 'ERROR'
          , lineas    = [
                f'  ❌ {fuente.upper()} — Error antes de escribir en Silver'
              , f'     Silver sin cambios — próximo run reintenta'
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
              , f'     Silver sin cambios — próximo run reintenta limpio'
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
            f'  ✅ {fuente.upper()} UPDATE-{modo.upper()} → {filas_update:,} filas'
          , f'  ✅ {fuente.upper()} INSERT           → {filas_insert:,} filas'
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
    Genera reporte, escribe TXT y envía email.

    Garantías:
      1. reporte generado en memoria con fallback mínimo
      2. TXT escrito desde memoria → si falla: WARNING
      3. Email recibe reporte desde memoria (contenido=)
         → sin leer disco, sin depender del TXT
      TXT y email son independientes entre sí.
      Ningún fallo aquí altera el estado real de Silver.
    """

    # ── PASO 1: Generar reporte en memoria ────────────────
    try:
        reporte = _generar_reporte(fuente, instancia, lineas, estado)
    except Exception as e:
        print(f"[{datetime.now()}] WARNING: _generar_reporte falló: {e}")
        reporte = (
            f"Silver {fuente.upper()} (instancia {instancia}) — "
            f"{estado} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"[Reporte no disponible: {e}]"
        )

    # ── PASO 2: Escribir TXT desde memoria ───────────────
    # Independiente del email — un fallo aquí no lo afecta
    try:
        escribir_log_txt(
            log_path  = log_path
          , vista     = f'silver_clients_{fuente}'
          , reporte   = reporte
          , dag_id    = DAG_ID
          , estado    = estado
          , notificar = False
        )
    except Exception as e:
        print(f"[{datetime.now()}] WARNING: TXT no guardado: {e}")

    # ── PASO 3: Enviar email desde memoria ────────────────
    # Recibe el reporte directamente — sin leer disco
    # Independiente del TXT — un fallo del TXT no lo afecta
    try:
        enviado = send_etl_notification(
            dag_id    = DAG_ID
          , status    = estado
          , contenido = reporte   # ← reporte en memoria ✅
          , extra_msg = f"Silver {fuente.upper()} (instancia {instancia})"
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
      , f'  ETL SILVER QUINCENAL — {DAG_ID}'
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
