# ═══════════════════════════════════════════════════════
# etl_base.py
# Objetivo: Motor de ejecución ETL reutilizable
# Carpeta: common/
# Versión: 2.4 — 2026-06-22 (blindaje conexiones + log detallado)
# ═══════════════════════════════════════════════════════
# CAMBIOS v2.1:
#   - NULL en SQL → None en Python para compatibilidad pymssql
#   - updatedAt y deletedAt se pasan como None desde Python
# CAMBIOS v2.2:
#   - dag_id como primer parámetro — identidad en logs
#   - traceback.format_exc() — error exacto con línea culpable
#   - try/except/finally — conexiones siempre se cierran
# CAMBIOS v2.3:
#   - get_max_id → SQL externalizado a sql/clients/get_max_id.sql
#   - cero SQL embebido en Python
# CAMBIOS v2.4:
#   - conn_origen/conn_destino = None antes del try
#   - Log detallado ANTES de raise: lote fallido + filas procesadas
#   - finally protegido con is not None — evita UnboundLocalError
#   - Nota: NO hay rollback — patrón INCREMENTAL, commit por lote
#     los lotes ya insertados se preservan ante un fallo parcial
# ═══════════════════════════════════════════════════════
import traceback
from datetime                                       import datetime
from airflow.providers.mysql.hooks.mysql            import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql  import MsSqlHook
from common.sql_loader                              import cargar_sql

BATCH_SIZE = 1000  # ← lotes de 1000 filas — patrón batch/Databricks

SQL_MAX_ID = "sql/clients/get_max_id.sql"


def get_max_id(mssql_conn_id: str, tabla_destino: str) -> int:
    """
    Obtiene el MAX(clientid) del destino SQL Server.
    Retorna 0 si la tabla está vacía.
    SQL externalizado en sql/clients/get_max_id.sql
    """
    query = cargar_sql(SQL_MAX_ID, tabla_destino=tabla_destino)
    hook  = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn  = None
    try:
        conn   = hook.get_conn()
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
    Ejecuta el ETL completo INCREMENTAL:
      1. Lee el .sql de SELECT y lo ejecuta en MariaDB
      2. Lee el .sql de INSERT y lo ejecuta en SQL Server
      3. Inserta en lotes de 1000 filas con executemany
      4. Commit por lote — patrón INCREMENTAL, no TRUNCATE+INSERT
         Los lotes ya insertados se preservan ante un fallo parcial

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
    hook_destino = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn_origen  = None
    conn_destino = None

    try:
        conn_origen  = hook_origen.get_conn()
        conn_destino = hook_destino.get_conn()

        cursor_origen  = conn_origen.cursor()
        cursor_destino = conn_destino.cursor()

        # ── SELECT en MariaDB ─────────────────────────────
        cursor_origen.execute(query_select)
        filas_insertadas = 0

        # ── INSERT en lotes de 1000 (executemany) ─────────
        # Commit por lote — INCREMENTAL: preserva lotes anteriores
        # ante un fallo parcial (diferencia clave vs etl_basephone)
        while True:
            filas = cursor_origen.fetchmany(BATCH_SIZE)
            if not filas:
                break

            # Agregar columnas de auditoría:
            # createdAt = etl_fecha, updatedAt = None, deletedAt = None
            lote = [fila + (etl_fecha, None, None) for fila in filas]

            # executemany → un solo roundtrip por lote de 1000
            cursor_destino.executemany(query_insert, lote)
            conn_destino.commit()
            filas_insertadas += len(lote)

        print(f"[DAG: {dag_id}] — ETL OK | Filas insertadas: {filas_insertadas}")
        return filas_insertadas

    except Exception as e:
        # ── Log detallado — diagnóstico completo ──────────
        print(f"[DAG: {dag_id}] — ERROR detectado en lote {filas_insertadas // BATCH_SIZE + 1}")
        print(f"[DAG: {dag_id}] — Filas procesadas antes del fallo: {filas_insertadas}")
        print(f"[DAG: {dag_id}] — {traceback.format_exc()}")
        # Nota: NO hay rollback — patrón INCREMENTAL
        # Los lotes ya commiteados se preservan
        raise  # ← re-lanza para que Airflow marque FAILED

    finally:
        if conn_origen  is not None: conn_origen.close()
        if conn_destino is not None: conn_destino.close()
        print(f"[DAG: {dag_id}] — Conexiones cerradas")
