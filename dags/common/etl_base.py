# ═══════════════════════════════════════════════════════
# etl_base.py
# Objetivo: Motor de ejecución ETL reutilizable
# Carpeta: common/
# Versión: 2.2 — 2026-06-19 (dag_id + traceback + blindaje conexiones)
# ═══════════════════════════════════════════════════════
# CAMBIOS v2.1:
#   - NULL en SQL → None en Python para compatibilidad pymssql
#   - updatedAt y deletedAt se pasan como None desde Python
# ═══════════════════════════════════════════════════════
# CAMBIOS v2.2:
#   - dag_id como primer parámetro — identidad en logs
#   - traceback.format_exc() — error exacto con línea culpable
#   - try/except/finally — conexiones siempre se cierran
import traceback
from datetime                                          import datetime
from airflow.hooks.mysql_hook                          import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql    import MsSqlHook
from common.sql_loader                                 import cargar_sql
from airflow.providers.mysql.hooks.mysql import MySqlHook  # ← nuevo

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
    dag_id          : str,    # ← El nuevo protagonista
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
    
    print(f"[DAG: {dag_id}] — Iniciando ETL | max_id: {max_id}")

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

            # Agregar columnas de auditoría:
            # createdAt = etl_fecha, updatedAt = None, deletedAt = None
            lote = [fila + (etl_fecha, None, None) for fila in filas]

            # executemany → un solo roundtrip por lote de 1000
            cursor_destino.executemany(query_insert, lote)
            conn_destino.commit()
            filas_insertadas += len(lote)

        return filas_insertadas
    
    except Exception as e:
        print(f"[DAG: {dag_id}] — ERROR: {traceback.format_exc()}")
        raise  # ← re-lanza para que Airflow marque FAILED

    finally:
        conn_origen.close()
        conn_destino.close()
        print(f"[DAG: {dag_id}] — Conexiones cerradas")


if __name__ == "__main__":
    import sys
    sys.path.insert(0, '/opt/airflow/dags')
    
    print("=" * 50)
    print("TEST 1 — Simulando FALLO")
    print("=" * 50)
    try:
        resultado = ejecutar_insert(
            dag_id          = "TEST_LOCAL"
          , mariadb_conn_id = "MariaDB"
          , mssql_conn_id   = "MSSQL_244"
          , sql_select      = "sql/clients/select_columna_mala.sql"  # ← no existe
          , sql_insert      = "sql/clients/insert_clientsbb_242.sql"
          , max_id          = 0
        )
    except Exception as e:
        print(f"✅ Fallo capturado correctamente: {type(e).__name__}")

    print("=" * 50)
    print("TEST 2 — Simulando ÉXITO")
    print("=" * 50)
    resultado = ejecutar_insert(
        dag_id          = "TEST_LOCAL"
      , mariadb_conn_id = "MariaDB"
      , mssql_conn_id   = "MSSQL_244"
      , sql_select      = "sql/clients/select_clientsbb_242.sql"
      , sql_insert      = "sql/clients/insert_clientsbb_242.sql"
      , max_id          = 0
    )
    print(f"✅ Filas insertadas: {resultado}")
