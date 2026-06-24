"""
profiling_phonefb.py
Profiling TRUNCATE+INSERT de phones FB: MariaDB 242 → SQL Server 244.
Replica la lógica de dag_profiling_phonefb + etl_basephone v3.0
sin depender de Airflow Hooks — usa .env directamente.
Cuando se promueva a DAG: reemplazar conexiones por BaseHook + MySqlHook.
Fecha: 2026-06-24
"""

import os
import time
import json
import traceback
import pyodbc
import pymysql
from datetime  import datetime
from dotenv    import load_dotenv

# ── Cargar variables de entorno ──────────────────────────
load_dotenv(dotenv_path=r"C:\Users\GUSA CAPITAL\Documents\DockersETL\.env")

MARIADB_HOST = os.environ["MARIADB_SOURCE_HOST"]
MARIADB_USER = os.environ["MARIADB_SOURCE_USER"]
MARIADB_PASS = os.environ["MARIADB_SOURCE_PASS"]
MARIADB_PORT = int(os.environ["MARIADB_SOURCE_PORT"])

MSSQL_HOST   = os.environ["MSSQL_PROD_HOST"]
MSSQL_USER   = os.environ["MSSQL_PROD_USER"]
MSSQL_PASS   = os.environ["MSSQL_PROD_PASS"]
MSSQL_PORT   = int(os.environ["MSSQL_PROD_PORT"])

# ── CONFIGURACIÓN — edita estas líneas ───────────────────
DAG_ID        = "profiling_phonefb"
VISTA_ORIGEN  = "db_general.vwpersonalinfofb"
TABLA_DESTINO = "source.Phonefb_temp23062026"   # ← tabla temporal de prueba
BATCH_SIZE    = 1000
# ─────────────────────────────────────────────────────────

# ── SQL inline — mismo contenido que los .sql externos ───
SQL_TRUNCATE = f"TRUNCATE TABLE {TABLA_DESTINO}"
SQL_SELECT   = f"SELECT clientid, PHONE FROM {VISTA_ORIGEN}"
SQL_INSERT   = f"""
    INSERT INTO {TABLA_DESTINO}
           ( clientid, phone, atInsert, atUpdate)
    VALUES ( ?, ?, ?, NULL)
"""


def main():
    print("=" * 55)
    print(f"  {DAG_ID} — Inicio")
    print("=" * 55)

    conn_origen  = None
    conn_destino = None

    try:
        # ── Conexión MariaDB 242 ─────────────────────────
        conn_origen = pymysql.connect(
            host     = MARIADB_HOST
           ,user     = MARIADB_USER
           ,password = MARIADB_PASS
           ,port     = MARIADB_PORT
           ,database = "db_general"
        )

        # ── Conexión SQL Server 244 ──────────────────────
        conn_destino = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
            f"DATABASE=DBGeneralDW;"
            f"UID={MSSQL_USER};"
            f"PWD={MSSQL_PASS};"
            f"TrustServerCertificate=yes;"
        )
        conn_destino.autocommit = False

        cursor_origen  = conn_origen.cursor()
        cursor_destino = conn_destino.cursor()
        cursor_destino.fast_executemany = True
        etl_fecha = datetime.now()

        # ── TRUNCATE — commit inmediato ──────────────────
        cursor_destino.execute(SQL_TRUNCATE)
        conn_destino.commit()
        print(f"\n[1] TRUNCATE {TABLA_DESTINO} OK")

        # ── SELECT origen ────────────────────────────────
        t_select_ini = time.perf_counter()
        cursor_origen.execute(SQL_SELECT)
        t_select_fin = time.perf_counter()
        print(f"[2] SELECT execute : {t_select_fin - t_select_ini:.2f}s")

        filas_insertadas    = 0
        tiempo_fetch_total  = 0.0
        tiempo_mem_total    = 0.0
        tiempo_insert_total = 0.0

        # ── INSERT en lotes ──────────────────────────────
        print(f"[3] Insertando en lotes de {BATCH_SIZE:,}...")
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
            cursor_destino.executemany(SQL_INSERT, lote)
            t_insert_fin = time.perf_counter()
            tiempo_insert_total += (t_insert_fin - t_insert_ini)

            filas_insertadas += len(lote)

        # ── COMMIT único — todo o nada ───────────────────
        conn_destino.commit()

        # ── Métricas finales ─────────────────────────────
        tiempo_total = tiempo_fetch_total + tiempo_mem_total + tiempo_insert_total
        rps          = filas_insertadas / tiempo_insert_total if tiempo_insert_total > 0 else 0

        print(f"\n{'=' * 55}")
        print(f"  PROFILING RESULTS")
        print(f"{'=' * 55}")
        print(f"  SELECT execute : {t_select_fin - t_select_ini:.2f}s")
        print(f"  FETCH  MariaDB : {tiempo_fetch_total:.2f}s")
        print(f"  MEM    Python  : {tiempo_mem_total:.2f}s")
        print(f"  INSERT SQL Svr : {tiempo_insert_total:.2f}s")
        print(f"  RPS            : {rps:,.0f} rows/sec")
        print(f"  TOTAL          : {tiempo_total:.2f}s")
        print(f"  Filas          : {filas_insertadas:,}")
        print(f"{'=' * 55}")

        metrics = {
            "dag_id"         : DAG_ID
           ,"tabla_destino"  : TABLA_DESTINO
           ,"driver"         : "pyodbc+fast_executemany"
           ,"batch_size"     : BATCH_SIZE
           ,"total_rows"     : filas_insertadas
           ,"select_seconds" : round(t_select_fin - t_select_ini, 2)
           ,"fetch_seconds"  : round(tiempo_fetch_total, 2)
           ,"mem_seconds"    : round(tiempo_mem_total, 2)
           ,"insert_seconds" : round(tiempo_insert_total, 2)
           ,"total_seconds"  : round(tiempo_total, 2)
           ,"rps"            : round(rps, 0)
        }
        print(f"\n[METRICS_JSON]: {json.dumps(metrics)}")
        print(f"\n  Proceso completado OK ✅")

    except Exception as e:
        print(f"\n  ❌ ERROR en lote {filas_insertadas // BATCH_SIZE + 1}")
        print(f"  Filas antes del fallo: {filas_insertadas:,}")
        print(f"  {traceback.format_exc()}")
        if conn_destino is not None:
            conn_destino.rollback()
            print(f"  ROLLBACK ejecutado — {TABLA_DESTINO} queda intacta")
        raise

    finally:
        if conn_origen  is not None: conn_origen.close()
        if conn_destino is not None: conn_destino.close()
        print("  Conexiones cerradas")


if __name__ == "__main__":
    main()
