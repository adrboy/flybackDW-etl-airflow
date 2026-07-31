# ═══════════════════════════════════════════════════════
# operations/generate_data.py
# Objetivo : Consolidar las 4 tablas → db_general.complete
# Servidor : 192.168.10.242  (todo interno en db_general)
# Versión  : 2.0 — 2026-07-30
# Nota     : Solo corre después de que Fase 1 esté completa
#            gusa_collections + flyback + buyback + vtw llenas
# ═══════════════════════════════════════════════════════
import traceback
from airflow.providers.mysql.hooks.mysql import MySqlHook
from common.db_connections                import ORIGEN_CONN_ID_242
from common.sql_loader                    import cargar_sql

# ── Rutas a los archivos SQL ─────────────────────────────
SQL_TRUNCATE = 'sql/datasync/truncate_complete.sql'
SQL_INSERT   = 'sql/datasync/insert_complete.sql'

SQL_LOG = "INSERT INTO db_general.log (description) VALUES ('insert complete');"


def generar_data(dag_id: str) -> None:
    """
    Trunca complete y la reconstruye con el UNION de las 4 tablas.
    Todo en db_general — una sola conexión al 242.

    Flujo:
        1. Trunca complete
        2. INSERT ... SELECT con UNION GC + FB + BB + VTW
        3. Commit
    """
    hook = MySqlHook(mysql_conn_id=ORIGEN_CONN_ID_242)
    conn = None

    try:
        conn = hook.get_conn()
        conn.autocommit = False
        cursor = conn.cursor()

        # ── 1. Truncar ───────────────────────────────────
        print(f"[{dag_id}] generate_data — truncando complete...")
        cursor.execute(cargar_sql(SQL_TRUNCATE))

        # ── 2. Consolidar las 4 tablas ───────────────────
        print(f"[{dag_id}] generate_data — consolidando GC + FB + BB + VTW...")
        cursor.execute(cargar_sql(SQL_INSERT))
        cursor.execute(SQL_LOG)

        conn.commit()
        print(f"[{dag_id}] generate_data — OK ✅  ({cursor.rowcount:,} filas en complete)")

    except Exception:
        if conn:
            conn.rollback()
        print(f"[{dag_id}] generate_data — ERROR ❌\n{traceback.format_exc()}")
        raise

    finally:
        if conn:
            conn.close()
