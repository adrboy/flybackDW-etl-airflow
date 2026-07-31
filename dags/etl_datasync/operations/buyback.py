# ═══════════════════════════════════════════════════════
# operations/buyback.py
# Objetivo : Sincronizar buyback.clients → db_general.buyback
# Origen   : 192.168.10.242  (buyback)
# Destino  : 192.168.10.242  (db_general)
# Versión  : 2.1 — 2026-07-30
# Cambios  : fecha_o_none() — validación de fechas en Python,
#            no en SQL (MySQLdb no soporta IF con %s en fechas)
# ═══════════════════════════════════════════════════════
import traceback
from airflow.providers.mysql.hooks.mysql import MySqlHook
from common.db_connections                import ORIGEN_CONN_ID_242
from common.sql_loader                    import cargar_sql

# ── Rutas a los archivos SQL ─────────────────────────────
SQL_TRUNCATE = 'sql/datasync/truncate_buyback.sql'
SQL_SELECT   = 'sql/datasync/select_buyback.sql'
SQL_INSERT   = 'sql/datasync/insert_buyback.sql'

SQL_LOG_INSERT = "INSERT INTO db_general.log (description) VALUES ('insert buyback');"

FECHA_INVALIDA = '0001-01-01'


def fecha_o_none(valor):
    """
    Retorna None si la fecha viene como '0001-01-01' (valor inválido
    que usa el C# original como centinela de fecha vacía).
    En cualquier otro caso retorna el valor tal cual.
    """
    if valor is None:
        return None
    if str(valor)[:10] <= FECHA_INVALIDA:
        return None
    return valor


def sincronizar_buyback(dag_id: str) -> int:
    """
    Lee buyback.clients (242) y sincroniza db_general.buyback (242).

    Flujo:
        1. Trunca destino
        2. Lee origen con SELECT externo
        3. Limpia fechas inválidas con fecha_o_none()
        4. Inserta con executemany (batch)
        5. Commit

    Retorna el número de filas insertadas.
    """
    hook = MySqlHook(mysql_conn_id=ORIGEN_CONN_ID_242)
    conn = None

    try:
        conn = hook.get_conn()
        conn.autocommit = False
        cursor = conn.cursor()

        # ── 1. Truncar destino ───────────────────────────
        print(f"[{dag_id}] buyback — truncando tabla...")
        cursor.execute(cargar_sql(SQL_TRUNCATE))

        # ── 2. Leer origen ───────────────────────────────
        print(f"[{dag_id}] buyback — leyendo buyback.clients...")
        cursor.execute(cargar_sql(SQL_SELECT))
        filas = cursor.fetchall()
        total = len(filas)
        print(f"[{dag_id}] buyback — {total:,} registros leídos")

        # ── 3. Limpiar fechas inválidas ──────────────────
        # sign = índice 18, activated = índice 19
        filas_preparadas = []
        for f in filas:
            fila = list(f)
            fila[18] = fecha_o_none(fila[18])   # sign
            fila[19] = fecha_o_none(fila[19])   # activated
            filas_preparadas.append(tuple(fila))

        # ── 4. Insertar en destino ───────────────────────
        cursor.executemany(cargar_sql(SQL_INSERT), filas_preparadas)
        cursor.execute(SQL_LOG_INSERT)

        conn.commit()
        print(f"[{dag_id}] buyback — OK ✅  ({total:,} filas)")
        return total

    except Exception:
        if conn:
            conn.rollback()
        print(f"[{dag_id}] buyback — ERROR ❌\n{traceback.format_exc()}")
        raise

    finally:
        if conn:
            conn.close()
