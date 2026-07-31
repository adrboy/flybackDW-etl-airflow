# ═══════════════════════════════════════════════════════
# operations/vacation_center.py
# Objetivo : Sincronizar vtw.p_data → db_general.vtw
# Origen   : 192.168.10.240  (vtw)
# Destino  : 192.168.10.242  (db_general)
# Versión  : 2.0 — 2026-07-30
# ═══════════════════════════════════════════════════════
import traceback
from airflow.providers.mysql.hooks.mysql import MySqlHook
from common.db_connections                import ORIGEN_CONN_ID_240, ORIGEN_CONN_ID_242
from common.sql_loader                    import cargar_sql

# ── Rutas a los archivos SQL ─────────────────────────────
SQL_TRUNCATE = 'sql/datasync/truncate_vacation_center.sql'
SQL_SELECT   = 'sql/datasync/select_vacation_center.sql'
SQL_INSERT   = 'sql/datasync/insert_vacation_center.sql'

SQL_LOG_INSERT = "INSERT INTO db_general.log (description) VALUES ('insert vtw');"

FECHA_INVALIDA = '0001-01-01'


def fecha_o_none(valor):
    """
    Retorna None si la fecha es inválida (centinela 0001-01-01 del C#).
    """
    if valor is None:
        return None
    if str(valor)[:10] <= FECHA_INVALIDA:
        return None
    return valor


def sincronizar_vacation_center(dag_id: str) -> int:
    """
    Lee vtw.p_data (240) y sincroniza db_general.vtw (242).

    Flujo:
        1. Trunca destino  (242)
        2. Lee origen      (240)
        3. Limpia capdata con fecha_o_none()
        4. Inserta batch   (242)
        5. Commit

    Retorna el número de filas insertadas.
    """
    hook_origen  = MySqlHook(mysql_conn_id=ORIGEN_CONN_ID_240)
    hook_destino = MySqlHook(mysql_conn_id=ORIGEN_CONN_ID_242)

    conn_origen  = None
    conn_destino = None

    try:
        conn_origen  = hook_origen.get_conn()
        conn_destino = hook_destino.get_conn()
        conn_destino.autocommit = False

        cur_origen  = conn_origen.cursor()
        cur_destino = conn_destino.cursor()

        # ── 1. Truncar destino ───────────────────────────
        print(f"[{dag_id}] vacation_center — truncando tabla...")
        cur_destino.execute(cargar_sql(SQL_TRUNCATE))

        # ── 2. Leer origen ───────────────────────────────
        print(f"[{dag_id}] vacation_center — leyendo vtw.p_data (240)...")
        cur_origen.execute(cargar_sql(SQL_SELECT))
        filas = cur_origen.fetchall()
        total = len(filas)
        print(f"[{dag_id}] vacation_center — {total:,} registros leídos")

        # ── 3. Limpiar fechas inválidas ──────────────────
        # capdata = índice 17
        filas_preparadas = []
        for f in filas:
            fila     = list(f)
            fila[17] = fecha_o_none(fila[17])   # capdata
            filas_preparadas.append(tuple(fila))

        # ── 4. Insertar en destino ───────────────────────
        cur_destino.executemany(cargar_sql(SQL_INSERT), filas_preparadas)
        cur_destino.execute(SQL_LOG_INSERT)

        conn_destino.commit()
        print(f"[{dag_id}] vacation_center — OK ✅  ({total:,} filas)")
        return total

    except Exception:
        if conn_destino:
            conn_destino.rollback()
        print(f"[{dag_id}] vacation_center — ERROR ❌\n{traceback.format_exc()}")
        raise

    finally:
        if conn_origen:  conn_origen.close()
        if conn_destino: conn_destino.close()
