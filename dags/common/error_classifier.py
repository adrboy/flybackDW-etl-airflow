# ═══════════════════════════════════════════════════════
# error_classifier.py
# Objetivo: Clasificar errores ETL y generar reporte
#           legible para log .txt y email
# Carpeta: common/
# Versión: 1.0 — 2026-08-24
# ═══════════════════════════════════════════════════════
# Categorías soportadas:
#   TRUNCADO_DATOS    → right truncation
#   TIPO_DECIMAL      → Converting decimal loses precision
#   TIPO_FECHA        → Datetime field overflow
#   DESBALANCE_COLS   → Expected X parameters, supplied Y
#   FALLA_CONEXION    → Can't connect / Connection refused
#   CREDENCIALES      → Access denied
#   ERROR_DESCONOCIDO → cualquier otro — incluye referencia Airflow
# ═══════════════════════════════════════════════════════

# ── Mapa de categorías ────────────────────────────────
_CATEGORIAS = [
    (
        "TRUNCADO DE DATOS"
      , ["right truncation", "string data"]
      , "Revisar longitud de columnas en destino SQL Server — usar _diagnosticar_lote para identificar la columna exacta"
    ),
    (
        "TIPO DECIMAL"
      , ["converting decimal", "loses precision"]
      , "Revisar precisión de columnas DECIMAL en destino SQL Server — ampliar escala (ej. decimal(11,3))"
    ),
    (
        "TIPO FECHA"
      , ["datetime field overflow", "datetime overflow"]
      , "Agregar CAST(columna AS DATE) en el SELECT origen — SQL Server no acepta datetime con hora en campo date"
    ),
    (
        "DESBALANCE DE COLUMNAS"
      , ["expected", "parameters, supplied"]
      , "El número de columnas del SELECT no coincide con el INSERT — revisar si el motor etl_base agrega columnas de auditoría"
    ),
    (
        "FALLA DE CONEXIÓN"
      , ["can't connect", "connection refused", "connection timeout", "timed out"]
      , "Verificar que el servidor de base de datos esté disponible y accesible desde Docker"
    ),
    (
        "CREDENCIALES"
      , ["access denied", "authentication failed", "login failed"]
      , "Verificar usuario y contraseña en las conexiones de Airflow"
    ),
]


def clasificar_error(mensaje_error: str) -> tuple:
    """
    Clasifica un error ETL por su mensaje.

    Returns:
        (categoria, accion) — strings listos para el reporte
    """
    mensaje_lower = mensaje_error.lower()
    for categoria, palabras_clave, accion in _CATEGORIAS:
        if any(k in mensaje_lower for k in palabras_clave):
            return categoria, accion

    return (
        "ERROR DESCONOCIDO"
      , "Revisar log completo de Airflow usando la referencia al final de este reporte"
    )


def _diagnosticar_columnas(lote: list) -> list:
    """
    Retorna lista de strings con columnas sospechosas del lote.
    Columnas con strings > 20 caracteres son candidatas a truncado.
    """
    lineas = []
    for i, fila in enumerate(lote):
        for j, valor in enumerate(fila):
            if isinstance(valor, str) and len(valor) > 20:
                vista = f"'{valor[:60]}{'...' if len(valor) > 60 else ''}'"
                lineas.append(
                    f"  Fila {i:>3} | Col {j:>2} | len {len(valor):>3} | {vista}"
                )
    return lineas


def generar_reporte_error(
    dag_id        : str
  , vista_origen  : str
  , tabla_destino : str
  , max_id        : int
  , filas_ok      : int
  , error         : Exception
  , run_id        : str  = None
  , task_id       : str  = None
  , attempt       : int  = None
  , lote          : list = None
) -> str:
    """
    Genera el reporte completo de error en texto plano.
    Listo para escribir al .txt y enviar por email.

    Args:
        dag_id        : ID del DAG
        vista_origen  : Vista MariaDB origen
        tabla_destino : Tabla SQL Server destino
        max_id        : MAX clientid antes del ETL
        filas_ok      : Filas insertadas antes del fallo
        error         : Excepción capturada
        run_id        : run_id de Airflow (opcional)
        task_id       : task_id de Airflow (opcional)
        attempt       : Número de intento de Airflow (opcional)
        lote          : Lote de datos que falló (opcional)

    Returns:
        String con el reporte completo
    """
    from datetime import datetime
    ahora         = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg_error     = str(error)
    categoria, accion = clasificar_error(msg_error)
    sep           = "═" * 52

    # ── Encabezado ────────────────────────────────────
    reporte = f"""
{sep}
  ETL REPORT — {dag_id}
{sep}
Fecha     : {ahora}
Origen    : {vista_origen}
Destino   : {tabla_destino}
Max ID    : {max_id:,}
Filas OK  : {filas_ok:,}
RESULTADO : ❌ FAILED
{sep}

══ ERROR {"═" * 44}
CATEGORÍA : {categoria}
MENSAJE   : {msg_error}
ACCIÓN    : {accion}
"""

    # ── Diagnóstico columnas (solo si hay lote y es truncado) ──
    if lote and "TRUNCADO" in categoria:
        lineas_diag = _diagnosticar_columnas(lote)
        if lineas_diag:
            reporte += f"""
══ DIAGNÓSTICO COLUMNAS {"═" * 29}
{chr(10).join(lineas_diag)}
"""

    # ── Referencia Airflow ────────────────────────────
    if run_id and task_id:
        log_path_airflow = (
            f"/opt/airflow/logs/dag_id={dag_id}"
            f"/run_id={run_id}"
            f"/task_id={task_id}"
            f"/attempt={attempt or 1}.log"
        )
        comando = (
            f'docker-compose exec airflow-scheduler cat "{log_path_airflow}"'
        )
        reporte += f"""
══ REFERENCIA AIRFLOW {"═" * 31}
run_id  : {run_id}
task    : {task_id}
attempt : {attempt or 1}
Comando :
  {comando}
"""

    reporte += f"\n{sep}\n"
    return reporte.strip()


def generar_reporte_success(
    dag_id        : str
  , vista_origen  : str
  , tabla_destino : str
  , max_id        : int
  , filas_ok      : int
  , segundos      : float
) -> str:
    """
    Genera el reporte de éxito en texto plano.
    """
    from datetime import datetime
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sep   = "═" * 52

    return f"""
{sep}
  ETL REPORT — {dag_id}
{sep}
Fecha     : {ahora}
Origen    : {vista_origen}
Destino   : {tabla_destino}
Max ID    : {max_id:,}
Filas OK  : {filas_ok:,}
Duración  : {segundos:.1f} segundos
RESULTADO : ✅ SUCCESS
{sep}
""".strip()
