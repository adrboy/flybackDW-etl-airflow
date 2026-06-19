# ═══════════════════════════════════════════════════════
# etl_base.py
# Objetivo: Motor de ejecución ETL reutilizable
# Carpeta: common/
# Versión: 2.0 — 2026-06-19 (SQL externalizado + executemany)
# ═══════════════════════════════════════════════════════
# CAMBIOS v2.0:
#   - SQL embebido eliminado → archivos .sql externos via sql_loader
#   - INSERT fila por fila → executemany en lotes de 1000
#   - Preparado para Databricks + Polars
# ═══════════════════════════════════════════════════════

from datetime                                          import datetime
from airflow.hooks.mysql_hook                          import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql    import MsSqlHook
from common.sql_loader                                 import cargar_sql

BATCH_SIZE = 1000  # ← lotes de 1000 filas — patrón batch/Databricks

def get_max_id(mssql_conn_id: str, tabla_destino: str) -> int:
    """
    Obtiene el MAX(clientid) del destino SQL Server.
    Retorna 0 si la tabla está vacía.
    """
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT ISNULL(MAX(clientid), 0) FROM {tabla_destino}")
        resultado = cursor.fetchone()
        return resultado[0]
    finally:
        conn.close()


def ejecutar_insert(
    mariadb_conn_id : str,
    mssql_conn_id   : str,
    sql_select      : str,   # ← ruta relativa al .sql de SELECT
    sql_insert      : str,   # ← ruta relativa al .sql de INSERT
    max_id          : int,
    etl_fecha       : datetime = None,
) -> int:
    """
    Ejecuta el ETL completo:
      1. Lee el .sql de SELECT y lo ejecuta en MariaDB
      2. Lee el .sql de INSERT y lo ejecuta en SQL Server
      3. Inserta en lotes de 1000 filas con executemany

    Args:
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

    # ── Cargar SQL externos ───────────────────────────────
    query_select = cargar_sql(sql_select, max_id=max_id)
    query_insert = cargar_sql(sql_insert)

    # ── Conexiones ────────────────────────────────────────
    hook_origen  = MySqlHook(mysql_conn_id=mariadb_conn_id)
    hook_destino = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn_origen  = hook_origen.get_conn()
    conn_destino = hook_destino.get_conn()

    try:
        cursor_origen  = conn_origen.cursor()
        cursor_destino = conn_destino.cursor()

        # ── SELECT en MariaDB ─────────────────────────────
        cursor_origen.execute(query_select)
        filas_insertadas = 0

        # ── INSERT en lotes de 1000 (executemany) ─────────
        while True:
            filas = cursor_origen.fetchmany(BATCH_SIZE)
            if not filas:
                break

            # Agregar etl_fecha a cada fila → columna createdAt
            lote = [fila + (etl_fecha,) for fila in filas]

            # executemany → un solo roundtrip por lote de 1000
            cursor_destino.executemany(query_insert, lote)
            conn_destino.commit()
            filas_insertadas += len(lote)

        return filas_insertadas

    finally:
        conn_origen.close()
        conn_destino.close()


if __name__ == "__main__":
    # ── Test de humo ─────────────────────────────────────
    print("etl_base.py v2.0 — test de humo")
    print(f"BATCH_SIZE: {BATCH_SIZE}")
    print("Usa cargar_sql() para cargar los .sql externos")
