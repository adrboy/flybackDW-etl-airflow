# ═══════════════════════════════════════════════════════
# datasync/operations/validate_country.py  — BACKUP 2026-07-30
# Objetivo : Normalizar country_code en db_general.complete
# Servidor : 192.168.10.242  (db_general)
# ═══════════════════════════════════════════════════════
import traceback
from airflow.providers.mysql.hooks.mysql import MySqlHook

CONN_GLOBAL = 'MariaDB'

SQL_SET_USA = """
    UPDATE db_general.complete SET country_code = 'USA'
    WHERE id IN (
        SELECT id FROM db_general.complete
        WHERE LENGTH(country_code) > 3 AND country_code REGEXP 'USA'
    )
"""

SQL_TRIM_OTHERS = """
    UPDATE db_general.complete SET country_code = LEFT(country_code, 3)
    WHERE id IN (
        SELECT id FROM db_general.complete
        WHERE LENGTH(country_code) > 3 AND country_code NOT REGEXP 'USA'
    )
"""


def validar_country(dag_id: str) -> None:
    hook = MySqlHook(mysql_conn_id=CONN_GLOBAL)
    conn = None
    try:
        conn   = hook.get_conn()
        cursor = conn.cursor()
        conn.autocommit = False
        print(f"[{dag_id}] validate_country — paso 1: forzar USA...")
        cursor.execute(SQL_SET_USA)
        print(f"[{dag_id}] validate_country — {cursor.rowcount:,} registros → USA")
        print(f"[{dag_id}] validate_country — paso 2: recortar otros países...")
        cursor.execute(SQL_TRIM_OTHERS)
        print(f"[{dag_id}] validate_country — {cursor.rowcount:,} registros recortados")
        conn.commit()
        print(f"[{dag_id}] validate_country — OK ✅")
    except Exception:
        if conn:
            conn.rollback()
        print(f"[{dag_id}] validate_country — ERROR ❌\n{traceback.format_exc()}")
        raise
    finally:
        if conn:
            conn.close()
