# ═══════════════════════════════════════════════════════
# audit_logger.py
# Objetivo: Registrar logs ETL en BD y archivo .txt
# Carpeta: common/
# Versión: 2.0 — 2026-08-24
# ═══════════════════════════════════════════════════════
# CAMBIOS v2.0:
#   - escribir_log_txt ahora recibe el reporte estructurado
#     generado por error_classifier.py
#   - Agrega send_etl_notification automático según estado
# ═══════════════════════════════════════════════════════
import pymysql
import os
from datetime               import datetime
from airflow.hooks.mysql_hook import MySqlHook
from common.email_notifier  import send_etl_notification


def registrar_log(
    paquete,
    vista_origen,
    tabla_destino,
    max_id_inicio,
    filas_insertadas,
    tipo_ejecucion,
    estado,
    mensaje_error,
    fecha_inicio,
    fecha_fin
):
    """Registra el resultado del ETL en la tabla etl_audit_log de MariaDB."""
    hook       = MySqlHook(mysql_conn_id='MariaDB')
    connection = hook.get_conn()

    try:
        with connection.cursor() as cursor:
            sql = """
                INSERT INTO flybackDW.etl_audit_log (
                    paquete, vista_origen, tabla_destino,
                    max_id_inicio, filas_insertadas, tipo_ejecucion,
                    estado, mensaje_error, fecha_inicio, fecha_fin
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                paquete, vista_origen, tabla_destino,
                max_id_inicio, filas_insertadas, tipo_ejecucion,
                estado, mensaje_error,
                fecha_inicio.strftime("%Y-%m-%d %H:%M:%S"),
                fecha_fin.strftime("%Y-%m-%d %H:%M:%S")
            ))
            connection.commit()
    finally:
        connection.close()


def escribir_log_txt(
    log_path  : str
  , vista     : str
  , reporte   : str          # ← reporte estructurado de error_classifier
  , dag_id    : str  = ""
  , estado    : str  = "OK"  # ← "OK" o "ERROR"
  , notificar : bool = True  # ← enviar email automático
) -> str:
    """
    Escribe el reporte ETL en archivo .txt y envía notificación por email.

    Args:
        log_path  : Carpeta donde guardar el .txt
        vista     : Nombre corto para el archivo (ej. 'clientsvc')
        reporte   : Texto del reporte generado por error_classifier
        dag_id    : ID del DAG para el email
        estado    : 'OK' o 'ERROR'
        notificar : Si True envía email automático

    Returns:
        Ruta completa del archivo .txt generado
    """
    timestamp     = datetime.now().strftime("%Y%m%d%H%M%S")
    nombre_archivo = f"etl_{vista}_FB_log_{timestamp}.txt"
    ruta_completa  = os.path.join(log_path, nombre_archivo)

    with open(ruta_completa, "w", encoding="utf-8") as f:
        f.write(reporte)

    # ── Notificación por email ────────────────────────────
    if notificar and dag_id:
        # SUCCESS: solo si quieres notificación — por defecto solo ERROR
        if estado == "ERROR":
            send_etl_notification(
                dag_id   = dag_id
              , status   = "ERROR"
              , log_path = ruta_completa
            )
        # Descomentar si también quieres email en SUCCESS:
        # elif estado == "OK":
        #     send_etl_notification(
        #         dag_id   = dag_id
        #       , status   = "OK"
        #       , log_path = ruta_completa
        #     )

    return ruta_completa
