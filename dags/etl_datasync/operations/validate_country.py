# ═══════════════════════════════════════════════════════
# operations/validate_country.py
# Objetivo : Normalizar country_code en db_general.complete
# Servidor : 192.168.10.242  (db_general)
# Versión  : 2.0 — 2026-07-30
# ═══════════════════════════════════════════════════════
import traceback
from airflow.providers.mysql.hooks.mysql import MySqlHook
from common.db_connections                import ORIGEN_CONN_ID_242
from common.sql_loader                    import cargar_sql

# ── Rutas a los archivos SQL ─────────────────────────────
SQL_USA  = 'sql/datasync/update_validate_country_usa.sql'
SQL_TRIM = 'sql/datasync/update_validate_country_trim.sql'


def validar_country(dag_id: str) -> None:
    """
    Normaliza country_code en complete en dos pasos:
        1. Registros con 'USA' en el string → country_code = 'USA'
        2. Resto con más de 3 chars         → country_code = LEFT(3)
    """
    hook = MySqlHook(mysql_conn_id=ORIGEN_CONN_ID_242)
    conn = None

    try:
        conn = hook.get_conn()
        conn.autocommit = False
        cursor = conn.cursor()

        # ── Paso 1: forzar USA ───────────────────────────
        print(f"[{dag_id}] validate_country — paso 1: forzar USA...")
        cursor.execute(cargar_sql(SQL_USA))
        print(f"[{dag_id}] validate_country — {cursor.rowcount:,} registros → USA")

        # ── Paso 2: recortar otros países ────────────────
        print(f"[{dag_id}] validate_country — paso 2: recortar otros países...")
        cursor.execute(cargar_sql(SQL_TRIM))
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
