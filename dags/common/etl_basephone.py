# ═══════════════════════════════════════════════════════
# etl_basephone.py
# Objetivo: Motor TRUNCATE+INSERT reutilizable para phones
# Carpeta: common/
# Versión: 2.0 — 2026-06-22 (executemany + SQL externo + dag_id + traceback)
# ═══════════════════════════════════════════════════════
# CAMBIOS v2.0:
#   - execute row-by-row → executemany por lotes de 1000
#   - SQL embebido → archivos externos sql/phones/
#   - dag_id como parámetro — identidad en logs
#   - traceback.format_exc() — error exacto con línea culpable
#   - try/except/finally — conexiones siempre se cierran
#   - Código comentado viejo eliminado — ver etl_basephone.py.bk
# ═══════════════════════════════════════════════════════
# PENDIENTE v3.0:
#   - Migrar a UPSERT cuando el origen tenga llave natural
#     por teléfono (hoy solo tiene clientid + PHONE)
# ═══════════════════════════════════════════════════════
import traceback
from datetime                                       import datetime
from airflow.providers.mysql.hooks.mysql            import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql  import MsSqlHook
from common.sql_loader                              import cargar_sql

BATCH_SIZE   = 1000
SQL_SELECT   = "sql/phones/select_phone.sql"
SQL_INSERT   = "sql/phones/insert_phone.sql"


def ejecutar_truncate_insert(
    dag_id          : str
  , mariadb_conn_id : str
  , mssql_conn_id   : str
  , vista_origen    : str
  , tabla_destino   : str
) -> int:
    """
    Ejecuta TRUNCATE + INSERT en lotes de 1000 filas con executemany.

    Args:
        dag_id          : Identificador del DAG para logs
        mariadb_conn_id : ID conexión Airflow → MariaDB origen
        mssql_conn_id   : ID conexión Airflow → SQL Server destino
        vista_origen    : Vista MariaDB origen (db_general.vwpersonalinfo{xx})
        tabla_destino   : Tabla SQL Server destino (source.Phone{xx})

    Returns:
        Total de filas insertadas
    """
    print(f"[DAG: {dag_id}] — Iniciando TRUNCATE+INSERT | destino: {tabla_destino}")

    # ── Cargar SQL externos ───────────────────────────────
    query_select = cargar_sql(SQL_SELECT, vista_origen=vista_origen)
    query_insert = cargar_sql(SQL_INSERT, tabla_destino=tabla_destino)

    # ── Conexiones ────────────────────────────────────────
    hook_origen  = MySqlHook(mysql_conn_id=mariadb_conn_id)
    hook_destino = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn_origen  = hook_origen.get_conn()
    conn_destino = hook_destino.get_conn()

    try:
        cursor_origen  = conn_origen.cursor()
        cursor_destino = conn_destino.cursor()
        etl_fecha      = datetime.now()

        # ── TRUNCATE destino ──────────────────────────────
        cursor_destino.execute(f"TRUNCATE TABLE {tabla_destino}")
        conn_destino.commit()
        print(f"[DAG: {dag_id}] — TRUNCATE {tabla_destino} OK")

        # ── SELECT origen ─────────────────────────────────
        cursor_origen.execute(query_select)
        filas_insertadas = 0

        # ── INSERT en lotes de 1000 (executemany) ─────────
        while True:
            filas = cursor_origen.fetchmany(BATCH_SIZE)
            if not filas:
                break

            # Agregar atInsert — atUpdate siempre NULL en TRUNCATE+INSERT
            lote = [fila + (etl_fecha,) for fila in filas]

            # executemany → un solo roundtrip por lote de 1000
            cursor_destino.executemany(query_insert, lote)
            conn_destino.commit()
            filas_insertadas += len(lote)

        print(f"[DAG: {dag_id}] — Filas insertadas: {filas_insertadas}")
        return filas_insertadas

    except Exception as e:
        print(f"[DAG: {dag_id}] — ERROR: {traceback.format_exc()}")
        raise  # ← re-lanza para que Airflow marque FAILED

    finally:
        conn_origen.close()
        conn_destino.close()
        print(f"[DAG: {dag_id}] — Conexiones cerradas")
