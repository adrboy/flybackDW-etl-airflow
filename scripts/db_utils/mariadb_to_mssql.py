"""
mariadb_to_mssql.py
Utilería para copiar datos de MariaDB → SQL Server.
Uso: ajusta SQL_QUERY y DEST_TABLE, ejecuta el script.
Creado: 2026-06-24
"""

import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ── Cargar variables de entorno ──────────────────────────────────────────────
load_dotenv(dotenv_path=r"C:\Users\GUSA CAPITAL\Documents\DockersETL\.env")

MARIADB_HOST = os.environ["MARIADB_SOURCE_HOST"]
MARIADB_USER = os.environ["MARIADB_SOURCE_USER"]
MARIADB_PASS = os.environ["MARIADB_SOURCE_PASS"]
MARIADB_PORT = os.environ["MARIADB_SOURCE_PORT"]

MSSQL_HOST   = os.environ["MSSQL_PROD_HOST"]
MSSQL_USER   = os.environ["MSSQL_PROD_USER"]
MSSQL_PASS   = os.environ["MSSQL_PROD_PASS"]
MSSQL_PORT   = os.environ["MSSQL_PROD_PORT"]

# ── CONFIGURACIÓN — edita solo estas dos líneas ──────────────────────────────
SQL_QUERY  = """
    SELECT
         clientid
        ,contractid
        ,FirstName
        ,LastName
        ,capdata
    FROM db_general.viewclientsfb
"""
DEST_TABLE  = "fbclients20260624"   # nombre de la tabla destino
DEST_SCHEMA = "temporal"            # schema destino en SQL Server
DEST_DB     = "DBGeneralDW"         # base de datos destino
CHUNK_SIZE  = 10_000
# ─────────────────────────────────────────────────────────────────────────────

def get_engines():
    maria = create_engine(
        f"mysql+pymysql://{MARIADB_USER}:{MARIADB_PASS}"
        f"@{MARIADB_HOST}:{MARIADB_PORT}/db_general"
    )
    mssql = create_engine(
        f"mssql+pyodbc://{MSSQL_USER}:{MSSQL_PASS}"
        f"@{MSSQL_HOST}:{MSSQL_PORT}/{DEST_DB}"
        f"?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        , fast_executemany=True
    )
    return maria, mssql


def main():
    print("=" * 55)
    print("  mariadb_to_mssql.py — Inicio")
    print("=" * 55)

    maria_engine, mssql_engine = get_engines()

    # 1. Leer desde MariaDB
    print(f"\n[1] Leyendo desde MariaDB 242...")
    t0 = time.perf_counter()
    df = pd.read_sql(SQL_QUERY, maria_engine)
    t1 = time.perf_counter()
    print(f"    Registros leídos : {len(df):,}")
    print(f"    Tiempo fetch     : {t1 - t0:.2f}s")

    if df.empty:
        print("\n    Sin datos — proceso terminado.")
        return

    # 2. Insertar en SQL Server
    print(f"\n[2] Insertando en [{DEST_DB}].[{DEST_SCHEMA}].[{DEST_TABLE}]...")
    t2 = time.perf_counter()
    df.to_sql(
        name       = DEST_TABLE
       ,con        = mssql_engine
       ,schema     = DEST_SCHEMA
       ,if_exists  = "append"      # "replace" para recrear, "append" para agregar
       ,index      = False
       ,chunksize  = CHUNK_SIZE
    )
    t3 = time.perf_counter()
    print(f"    Registros insertados : {len(df):,}")
    print(f"    Tiempo insert        : {t3 - t2:.2f}s")
    print(f"    Tiempo total         : {t3 - t0:.2f}s")

    print("\n" + "=" * 55)
    print("  Proceso completado OK ✅")
    print("=" * 55)


if __name__ == "__main__":
    main()
