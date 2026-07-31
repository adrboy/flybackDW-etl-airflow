# ═══════════════════════════════════════════════════════
# operations/log_estadisticas.py
# Objetivo : Registrar conteos de cada BD origen en
#            complete_details — historial de cada run
# Servidores:
#   240 → GC (financiamiento.credits) y VTW (vtw.p_data)
#   242 → FB (customers.fb_clients) y BB (buyback.clients)
# Versión  : 1.1 — 2026-07-30
# Nota     : Solo corre si toda la Fase 1 y Fase 2 salieron OK
#            Fiel al C# GenerateLogsRegistros()
# ═══════════════════════════════════════════════════════
import traceback
from airflow.providers.mysql.hooks.mysql import MySqlHook
from common.db_connections                import ORIGEN_CONN_ID_240, ORIGEN_CONN_ID_242
from common.sql_loader                    import cargar_sql

# ── Rutas a los archivos SQL ─────────────────────────────
SQL_INSERT_UPDATE_TABLES    = 'sql/datasync/insert_update_tables.sql'
SQL_COUNT_GC                = 'sql/datasync/select_count_gc.sql'
SQL_COUNT_FB                = 'sql/datasync/select_count_fb.sql'
SQL_COUNT_BB                = 'sql/datasync/select_count_bb.sql'
SQL_COUNT_VTW               = 'sql/datasync/select_count_vtw.sql'
SQL_UPDATE_COMPLETE_DETAILS = 'sql/datasync/update_complete_details.sql'


def _contar(cursor, sql: str) -> int:
    """Ejecuta un COUNT y retorna el entero."""
    cursor.execute(sql)
    fila = cursor.fetchone()
    return int(fila[0]) if fila else 0


def registrar_estadisticas(dag_id: str) -> None:
    """
    1. Inserta en update_tables → trigger crea fila en complete_details
    2. Cuenta clientes en cada BD origen (GC, FB, BB, VTW)
    3. Actualiza complete_details con los conteos
    4. Commit

    Mapa de servidores:
        240 → GC (financiamiento.credits), VTW (vtw.p_data)
        242 → FB (customers.fb_clients),   BB  (buyback.clients)
    """
    hook_240 = MySqlHook(mysql_conn_id=ORIGEN_CONN_ID_240)
    hook_242 = MySqlHook(mysql_conn_id=ORIGEN_CONN_ID_242)

    conn_240 = None
    conn_242 = None

    try:
        conn_240 = hook_240.get_conn()
        conn_242 = hook_242.get_conn()
        conn_242.autocommit = False

        cur_240 = conn_240.cursor()
        cur_242 = conn_242.cursor()

        # ── 1. Generar update_id ─────────────────────────
        print(f"[{dag_id}] log_estadisticas — registrando run en update_tables...")
        cur_242.execute(cargar_sql(SQL_INSERT_UPDATE_TABLES))
        update_id = conn_242.insert_id()
        print(f"[{dag_id}] log_estadisticas — update_id: {update_id}")

        # ── 2. Contar en cada origen ─────────────────────
        print(f"[{dag_id}] log_estadisticas — contando registros en orígenes...")
        gc  = _contar(cur_240, cargar_sql(SQL_COUNT_GC))   # 240 — financiamiento
        vtw = _contar(cur_240, cargar_sql(SQL_COUNT_VTW))  # 240 — vtw
        fb  = _contar(cur_242, cargar_sql(SQL_COUNT_FB))   # 242 — customers
        bb  = _contar(cur_242, cargar_sql(SQL_COUNT_BB))   # 242 — buyback

        print(f"[{dag_id}] log_estadisticas — GC:{gc:,}  FB:{fb:,}  BB:{bb:,}  VTW:{vtw:,}")

        # ── 3. Actualizar complete_details ───────────────
        sql_update = cargar_sql(
            SQL_UPDATE_COMPLETE_DETAILS,
            update_id = update_id,
            gc        = gc,
            fb        = fb,
            bb        = bb,
            vtw       = vtw,
        )
        cur_242.execute(sql_update)

        conn_242.commit()
        print(f"[{dag_id}] log_estadisticas — OK ✅  (update_id: {update_id})")

    except Exception:
        if conn_242:
            conn_242.rollback()
        print(f"[{dag_id}] log_estadisticas — ERROR ❌\n{traceback.format_exc()}")
        raise

    finally:
        if conn_240: conn_240.close()
        if conn_242: conn_242.close()
