# ═══════════════════════════════════════════════════════
# etl_base.py
# Objetivo: Motor de ejecución ETL INCREMENTAL reutilizable
# Carpeta: common/
# Versión: 3.2 — 2026-08-24
# ═══════════════════════════════════════════════════════
# CAMBIOS v2.1: NULL → None para compatibilidad pymssql
# CAMBIOS v2.2: dag_id + traceback + blindaje conexiones
# CAMBIOS v2.3: get_max_id → SQL externo
# CAMBIOS v2.4: blindaje None check + log detallado en except
# CAMBIOS v3.0:
#   - MsSqlHook → pyodbc directo con fast_executemany = True
#   - Credenciales via BaseHook.get_connection() — seguro
#   - msodbcsql18 + TrustServerCertificate para SQL Server 2022
#   - get_max_id también migrado a pyodbc
#   - Placeholders %s → ? (sintaxis pyodbc)
#   - Nota: NO hay rollback — patrón INCREMENTAL
#     los lotes ya commiteados se preservan ante fallo parcial
# CAMBIOS v3.1:
#   - Diagnóstico fila por fila en except — identifica columna
#     y valor exacto que causó el error de truncado o tipo
# CAMBIOS v3.2:
#   - Integración con error_classifier.py
#   - Reporte estructurado SUCCESS/FAILED en log .txt y email
#   - Referencia Airflow automática en reporte de error
#   - Parámetros vista_origen + tabla_destino para reporte
# ═══════════════════════════════════════════════════════
import traceback
import time
import pyodbc
from datetime                                import datetime
from airflow.hooks.base                      import BaseHook
from airflow.providers.mysql.hooks.mysql     import MySqlHook
from common.sql_loader                       import cargar_sql
from common.error_classifier                 import generar_reporte_error, generar_reporte_success

BATCH_SIZE = 1000
SQL_MAX_ID = "sql/clients/get_max_id.sql"


def _get_pyodbc_conn(mssql_conn_id: str):
    """Crea conexión pyodbc usando credenciales de Airflow."""
    conn_data = BaseHook.get_connection(mssql_conn_id)
    conn_str  = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={conn_data.host};"
        f"DATABASE={conn_data.schema};"
        f"UID={conn_data.login};"
        f"PWD={conn_data.password};"
        f"TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str)
    conn.autocommit = False
    return conn


def get_max_id(mssql_conn_id: str, tabla_destino: str) -> int:
    """
    Obtiene el MAX(clientid) del destino SQL Server via pyodbc.
    Retorna 0 si la tabla está vacía.
    """
    query = cargar_sql(SQL_MAX_ID, tabla_destino=tabla_destino)
    conn  = None
    try:
        conn   = _get_pyodbc_conn(mssql_conn_id)
        cursor = conn.cursor()
        cursor.execute(query)
        resultado = cursor.fetchone()
        return resultado[0]
    finally:
        if conn is not None:
            conn.close()


def ejecutar_insert(
    dag_id          : str
  , mariadb_conn_id : str
  , mssql_conn_id   : str
  , sql_select      : str
  , sql_insert      : str
  , max_id          : int
  , vista_origen    : str      = ""     # ← para reporte
  , tabla_destino   : str      = ""     # ← para reporte
  , etl_fecha       : datetime = None
  , airflow_context : dict     = None   # ← contexto Airflow para referencia en error
) -> tuple:
    """
    Ejecuta el ETL completo INCREMENTAL con pyodbc + fast_executemany.
    Commit por lote — patrón INCREMENTAL: preserva lotes anteriores
    ante un fallo parcial.

    Args:
        dag_id          : Identificador del DAG para logs
        mariadb_conn_id : ID conexión Airflow → MariaDB origen
        mssql_conn_id   : ID conexión Airflow → SQL Server destino
        sql_select      : Ruta relativa al archivo SELECT .sql
        sql_insert      : Ruta relativa al archivo INSERT .sql
        max_id          : MAX(clientid) del destino para filtrar
        vista_origen    : Nombre de la vista origen (para reporte)
        tabla_destino   : Nombre de la tabla destino (para reporte)
        etl_fecha       : Fecha de ejecución ETL (default: NOW)
        airflow_context : Contexto de Airflow — para referencia en error

    Returns:
        (filas_insertadas, reporte) — int + string del reporte
    """
    if etl_fecha is None:
        etl_fecha = datetime.now()

    print(f"[DAG: {dag_id}] — Iniciando ETL | max_id: {max_id}")

    # ── Cargar SQL externos ───────────────────────────────
    query_select = cargar_sql(sql_select, max_id=max_id)
    query_insert = cargar_sql(sql_insert)

    # ── Conexiones ────────────────────────────────────────
    hook_origen  = MySqlHook(mysql_conn_id=mariadb_conn_id)
    conn_origen  = None
    conn_destino = None
    filas_insertadas = 0
    lote             = []
    inicio           = time.time()

    # ── Extraer referencia Airflow si viene el contexto ───
    run_id  = None
    task_id = None
    attempt = None
    if airflow_context:
        try:
            run_id  = airflow_context.get("run_id")
            task_id = airflow_context.get("task_instance").task_id
            attempt = airflow_context.get("task_instance").try_number
        except Exception:
            pass

    try:
        conn_origen  = hook_origen.get_conn()
        conn_destino = _get_pyodbc_conn(mssql_conn_id)

        cursor_origen  = conn_origen.cursor()
        cursor_destino = conn_destino.cursor()
        cursor_destino.fast_executemany = True

        # ── SELECT en MariaDB ─────────────────────────────
        cursor_origen.execute(query_select)

        # ── INSERT en lotes ───────────────────────────────
        while True:
            filas = cursor_origen.fetchmany(BATCH_SIZE)
            if not filas:
                break

            lote = [fila + (etl_fecha, None, None) for fila in filas]
            cursor_destino.executemany(query_insert, lote)
            conn_destino.commit()
            filas_insertadas += len(lote)

        # ── Reporte SUCCESS ───────────────────────────────
        segundos = time.time() - inicio
        reporte  = generar_reporte_success(
            dag_id        = dag_id
          , vista_origen  = vista_origen
          , tabla_destino = tabla_destino
          , max_id        = max_id
          , filas_ok      = filas_insertadas
          , segundos      = segundos
        )
        print(f"[DAG: {dag_id}] — ETL OK | Filas: {filas_insertadas:,} | {segundos:.1f}s")
        return filas_insertadas, reporte

    except Exception as e:
        # ── Reporte FAILED ────────────────────────────────
        reporte = generar_reporte_error(
            dag_id        = dag_id
          , vista_origen  = vista_origen
          , tabla_destino = tabla_destino
          , max_id        = max_id
          , filas_ok      = filas_insertadas
          , error         = e
          , run_id        = run_id
          , task_id       = task_id
          , attempt       = attempt
          , lote          = lote
        )
        print(reporte)
        raise

    finally:
        if conn_origen  is not None: conn_origen.close()
        if conn_destino is not None: conn_destino.close()
        print(f"[DAG: {dag_id}] — Conexiones cerradas")
