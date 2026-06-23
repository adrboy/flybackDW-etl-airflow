# ═══════════════════════════════════════════════════════
# etl_base.py
# Objetivo: Motor de ejecución ETL INCREMENTAL reutilizable
# Carpeta: common/
# Versión: 3.0 — 2026-06-23 (pyodbc + fast_executemany = True)
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
# ═══════════════════════════════════════════════════════
import traceback
import pyodbc
from datetime                                import datetime
from airflow.hooks.base                      import BaseHook
from airflow.providers.mysql.hooks.mysql     import MySqlHook
from common.sql_loader                       import cargar_sql

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
  , sql_select      : str    # ← ruta relativa al .sql de SELECT
  , sql_insert      : str    # ← ruta relativa al .sql de INSERT
  , max_id          : int
  , etl_fecha       : datetime = None
) -> int:
    """
    Ejecuta el ETL completo INCREMENTAL con pyodbc + fast_executemany.
    Commit por lote — patrón INCREMENTAL: preserva lotes anteriores
    ante un fallo parcial (diferencia clave vs etl_basephone).

    Args:
        dag_id          : Identificador del DAG para logs
        mariadb_conn_id : ID conexión Airflow → MariaDB origen
        mssql_conn_id   : ID conexión Airflow → SQL Server destino
        sql_select      : Ruta relativa al archivo SELECT .sql
        sql_insert      : Ruta relativa al archivo INSERT .sql
        max_id          : MAX(clientid) del destino para filtrar
        etl_fecha       : Fecha de ejecución ETL (default: NOW)

    Returns:
        Total de filas insertadas
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

    try:
        conn_origen  = hook_origen.get_conn()
        conn_destino = _get_pyodbc_conn(mssql_conn_id)

        cursor_origen  = conn_origen.cursor()
        cursor_destino = conn_destino.cursor()
        cursor_destino.fast_executemany = True   # ← alto rendimiento

        # ── SELECT en MariaDB ─────────────────────────────
        cursor_origen.execute(query_select)
        filas_insertadas = 0

        # ── INSERT en lotes (fast_executemany) ────────────
        # Commit por lote — INCREMENTAL
        while True:
            filas = cursor_origen.fetchmany(BATCH_SIZE)
            if not filas:
                break

            # createdAt = etl_fecha, updatedAt = None, deletedAt = None
            lote = [fila + (etl_fecha, None, None) for fila in filas]

            cursor_destino.executemany(query_insert, lote)
            conn_destino.commit()
            filas_insertadas += len(lote)

        print(f"[DAG: {dag_id}] — ETL OK | Filas insertadas: {filas_insertadas:,}")
        return filas_insertadas

    except Exception as e:
        print(f"[DAG: {dag_id}] — ERROR detectado en lote {filas_insertadas // BATCH_SIZE + 1}")
        print(f"[DAG: {dag_id}] — Filas procesadas antes del fallo: {filas_insertadas}")
        print(f"[DAG: {dag_id}] — {traceback.format_exc()}")
        # NO rollback — INCREMENTAL, preservar lotes ya commiteados
        raise

    finally:
        if conn_origen  is not None: conn_origen.close()
        if conn_destino is not None: conn_destino.close()
        print(f"[DAG: {dag_id}] — Conexiones cerradas")
