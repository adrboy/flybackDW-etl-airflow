"""
insert_huerfanos_clientsfb.py
Inserta registros huérfanos de viewclientsfb (MariaDB 242)
hacia source.clientsfb (SQL Server 244).
Usa el mismo SELECT/INSERT del ETL de producción dag_clientsfb_242.
Fecha: 2026-06-24
Clientids huérfanos: 151099, 151100, 190747, 286858, 286859
"""

import os
import time
import pyodbc
import pymysql
from dotenv import load_dotenv

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

# ── Clientids huérfanos a insertar ───────────────────────
CLIENTIDS = (151099, 151100, 190747, 286858, 286859)
PLACEHOLDERS = ','.join(['%s'] * len(CLIENTIDS))

# ── Mismo SELECT que producción — solo filtrando huérfanos
SQL_SELECT = f"""
SELECT productid
     , contractid
     , clientid
     , email
     , capdata
     , FirstName
     , LastName
     , countrycode
     , country
     , Estate
     , Ciudad
     , address
     , zip
     , Corpcode
     , Corp
     , ingreso
     , egreso
     , rank
     , EstatusN
     , EstatusL
FROM db_general.viewclientsfb
WHERE clientid IN ({PLACEHOLDERS})
ORDER BY clientid
"""

# ── Mismo INSERT que producción ──────────────────────────
SQL_INSERT = """
INSERT INTO source.clientsfb
       ( productid, contractid, clientid, email, capdata
       , FirstName, LastName, countrycode, country, Estate
       , Ciudad, address, zip, Corpcode, Corp
       , ingreso, egreso, rank, EstatusN, EstatusL
       , createdAt, updatedAt, deletedAt)
VALUES ( ?, ?, ?, ?, ?
       , ?, ?, ?, ?, ?
       , ?, ?, ?, ?, ?
       , ?, ?, ?, ?, ?
       , ?, ?, ?)
"""

def main():
    print("=" * 55)
    print("  insert_huerfanos_clientsfb.py — Inicio")
    print("=" * 55)

    # ── Conexión MariaDB 242 ─────────────────────────────
    maria_conn = pymysql.connect(
        host     = MARIADB_HOST
       ,user     = MARIADB_USER
       ,password = MARIADB_PASS
       ,port     = MARIADB_PORT
       ,database = "db_general"
    )

    # ── Conexión SQL Server 244 ──────────────────────────
    mssql_conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={MSSQL_HOST},{MSSQL_PORT};"
        f"DATABASE=DBGeneralDW;"
        f"UID={MSSQL_USER};"
        f"PWD={MSSQL_PASS};"
        f"TrustServerCertificate=yes;"
    )
    mssql_conn.autocommit = False

    try:
        # Paso 1 — Leer huérfanos desde MariaDB
        print(f"\n[1] Leyendo {len(CLIENTIDS)} huérfanos desde MariaDB 242...")
        t0 = time.perf_counter()
        with maria_conn.cursor() as cur:
            cur.execute(SQL_SELECT, CLIENTIDS)
            rows = cur.fetchall()
        t1 = time.perf_counter()
        print(f"    Registros encontrados : {len(rows)}")
        print(f"    Tiempo fetch          : {t1 - t0:.2f}s")

        if not rows:
            print("\n    Sin datos — los clientids no existen en viewclientsfb.")
            return

        # Preview antes de insertar
        print(f"\n    Preview de registros a insertar:")
        for r in rows:
            print(f"    clientid={r[2]} | contractid={r[1]} | {r[5]} {r[6]}")

        # Paso 2 — Insertar en SQL Server con mismo patrón producción
        print(f"\n[2] Insertando en source.clientsfb (SQL Server 244)...")

        # Agregar NULLs para createdAt, updatedAt, deletedAt
        rows_insert = [r + (None, None, None) for r in rows]

        t2 = time.perf_counter()
        cursor = mssql_conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(SQL_INSERT, rows_insert)
        mssql_conn.commit()
        t3 = time.perf_counter()

        print(f"    Registros insertados : {len(rows_insert)}")
        print(f"    Tiempo insert        : {t3 - t2:.2f}s")
        print(f"    Tiempo total         : {t3 - t0:.2f}s")

        print("\n" + "=" * 55)
        print("  Proceso completado OK ✅")
        print("=" * 55)

    except Exception as e:
        mssql_conn.rollback()
        print(f"\n  ❌ ERROR — rollback ejecutado")
        print(f"  {str(e)}")
        raise

    finally:
        maria_conn.close()
        mssql_conn.close()


if __name__ == "__main__":
    main()
