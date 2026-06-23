# ═══════════════════════════════════════════════════════
# etl_basephone.py
# Objetivo: Motor TRUNCATE+INSERT reutilizable para phones
# Carpeta: common/
# Versión: 3.0 — 2026-06-23 (pyodbc + fast_executemany = True)
# ═══════════════════════════════════════════════════════
# CAMBIOS v2.0: execute row-by-row → executemany + SQL externo + dag_id
# CAMBIOS v2.1: Transacción única — commit al final, rollback si falla
# CAMBIOS v2.2: Log detallado ANTES del rollback
# CAMBIOS v2.3: TRUNCATE → SQL externo + rollback seguro con None check
# CAMBIOS v2.4: Profiling fetch + mem + insert + RPS
# CAMBIOS v2.5: SELECT 1 sync + JSON metrics + BATCH_SIZE variable
# CAMBIOS v3.0:
#   - MsSqlHook → pyodbc directo con fast_executemany = True
#   - Credenciales obtenidas de forma segura via BaseHook.get_connection()
#   - msodbcsql18 + TrustServerCertificate para SQL Server 2022
#   - SELECT 1 eliminado — pyodbc con fast_executemany es síncrono real
# ═══════════════════════════════════════════════════════
# PENDIENTE v4.0:
#   - Migrar a UPSERT cuando el origen tenga llave natural
#     por teléfono (hoy solo tiene clientid + PHONE)
# ═══════════════════════════════════════════════════════
import traceback
import time
import json
import pyodbc
from datetime                                import datetime
from airflow.hooks.base                      import BaseHook
from airflow.providers.mysql.hooks.mysql     import MySqlHook
from common.sql_loader                       import cargar_sql

BATCH_SIZE   = 1000
SQL_SELECT   = "sql/phones/select_phone.sql"
SQL_INSERT   = "sql/phones/insert_phone.sql"
SQL_TRUNCATE = "sql/phones/truncate_phone.sql"


def ejecutar_truncate_insert(
    dag_id          : str
  , mariadb_conn_id : str
  , mssql_conn_id   : str
  , vista_origen    : str
  , tabla_destino   : str
) -> int:
    """
    Ejecuta TRUNCATE + INSERT en lotes con pyodbc + fast_executemany = True.
    Transacción única — commit al final, rollback seguro si falla.
    Profiling: fetch + mem + insert + RPS + JSON metrics.
    """
    print(f"[DAG: {dag_id}] — Iniciando TRUNCATE+INSERT | destino: {tabla_destino}")

    # ── Cargar SQL externos ───────────────────────────────
    query_truncate = cargar_sql(SQL_TRUNCATE, tabla_destino=tabla_destino)
    query_select   = cargar_sql(SQL_SELECT,   vista_origen=vista_origen)
    query_insert   = cargar_sql(SQL_INSERT,   tabla_destino=tabla_destino)

    # ── Conexión MariaDB via Hook ─────────────────────────
    hook_origen  = MySqlHook(mysql_conn_id=mariadb_conn_id)
    conn_origen  = None
    conn_destino = None

    try:
        conn_origen = hook_origen.get_conn()

        # ── Conexión SQL Server via pyodbc + fast_executemany ─
        conn_data = BaseHook.get_connection(mssql_conn_id)
        conn_str  = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={conn_data.host};"
            f"DATABASE={conn_data.schema};"
            f"UID={conn_data.login};"
            f"PWD={conn_data.password};"
            f"TrustServerCertificate=yes;"
        )
        conn_destino              = pyodbc.connect(conn_str)
        conn_destino.autocommit   = False

        cursor_origen  = conn_origen.cursor()
        cursor_destino = conn_destino.cursor()
        cursor_destino.fast_executemany = True   # ← alto rendimiento
        etl_fecha = datetime.now()

        # ── TRUNCATE destino — commit inmediato ───────────
        cursor_destino.execute(query_truncate)
        conn_destino.commit()
        print(f"[DAG: {dag_id}] — TRUNCATE {tabla_destino} OK")

        # ── SELECT origen ─────────────────────────────────
        t_select_ini = time.perf_counter()
        cursor_origen.execute(query_select)
        t_select_fin = time.perf_counter()
        print(f"[DAG: {dag_id}] — PROFILING SELECT execute : {t_select_fin - t_select_ini:.2f}s")

        filas_insertadas    = 0
        tiempo_fetch_total  = 0.0
        tiempo_mem_total    = 0.0
        tiempo_insert_total = 0.0

        # ── INSERT en lotes (fast_executemany) ────────────
        while True:

            t_fetch_ini = time.perf_counter()
            filas = cursor_origen.fetchmany(BATCH_SIZE)
            t_fetch_fin = time.perf_counter()
            tiempo_fetch_total += (t_fetch_fin - t_fetch_ini)

            if not filas:
                break

            t_mem_ini = time.perf_counter()
            lote = [fila + (etl_fecha,) for fila in filas]
            t_mem_fin = time.perf_counter()
            tiempo_mem_total += (t_mem_fin - t_mem_ini)

            t_insert_ini = time.perf_counter()
            cursor_destino.executemany(query_insert, lote)
            t_insert_fin = time.perf_counter()
            tiempo_insert_total += (t_insert_fin - t_insert_ini)

            filas_insertadas += len(lote)

        # ── COMMIT único al final — todo o nada ───────────
        conn_destino.commit()

        # ── Métricas finales ──────────────────────────────
        tiempo_total = tiempo_fetch_total + tiempo_mem_total + tiempo_insert_total
        rps          = filas_insertadas / tiempo_insert_total if tiempo_insert_total > 0 else 0

        print(f"[DAG: {dag_id}] — PROFILING FETCH  MariaDB  : {tiempo_fetch_total:.2f}s")
        print(f"[DAG: {dag_id}] — PROFILING MEM    Python    : {tiempo_mem_total:.2f}s")
        print(f"[DAG: {dag_id}] — PROFILING INSERT SQL Server: {tiempo_insert_total:.2f}s")
        print(f"[DAG: {dag_id}] — PROFILING RPS             : {rps:,.0f} rows/sec")
        print(f"[DAG: {dag_id}] — PROFILING TOTAL           : {tiempo_total:.2f}s | Filas: {filas_insertadas:,}")

        metrics = {
            "dag_id"         : dag_id
          , "tabla_destino"  : tabla_destino
          , "driver"         : "pyodbc+fast_executemany"
          , "batch_size"     : BATCH_SIZE
          , "total_rows"     : filas_insertadas
          , "select_seconds" : round(t_select_fin - t_select_ini, 2)
          , "fetch_seconds"  : round(tiempo_fetch_total, 2)
          , "mem_seconds"    : round(tiempo_mem_total, 2)
          , "insert_seconds" : round(tiempo_insert_total, 2)
          , "total_seconds"  : round(tiempo_total, 2)
          , "rps"            : round(rps, 0)
        }
        print(f"[METRICS_JSON]: {json.dumps(metrics)}")

        print(f"[DAG: {dag_id}] — COMMIT OK | Filas insertadas: {filas_insertadas:,}")
        return filas_insertadas

    except Exception as e:
        print(f"[DAG: {dag_id}] — ERROR detectado en lote {filas_insertadas // BATCH_SIZE + 1}")
        print(f"[DAG: {dag_id}] — Filas procesadas antes del fallo: {filas_insertadas}")
        print(f"[DAG: {dag_id}] — {traceback.format_exc()}")
        if conn_destino is not None:
            conn_destino.rollback()
            print(f"[DAG: {dag_id}] — ROLLBACK ejecutado — {tabla_destino} queda intacta")
        raise

    finally:
        if conn_origen  is not None: conn_origen.close()
        if conn_destino is not None: conn_destino.close()
        print(f"[DAG: {dag_id}] — Conexiones cerradas")
