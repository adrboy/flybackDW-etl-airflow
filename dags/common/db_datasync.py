# ═══════════════════════════════════════════════════════
# common/db_datasync.py
# Objetivo : Motor de sincronización MariaDB → MariaDB
#            Reutilizable para todas las operaciones de
#            etl_datasync — nadie más instancia MySqlHook
# Versión  : 1.4 — 2026-07-31
# Cambios  : mysql-connector-python en lugar de MySQLdb
#            executemany optimizado — un solo INSERT multi-values
#            BATCH_SIZE 50000 — menos roundtrips
# ═══════════════════════════════════════════════════════
import traceback
import mysql.connector
from airflow.hooks.base import BaseHook
from common.sql_loader   import cargar_sql

FECHA_INVALIDA = '0001-01-01'
BATCH_SIZE     = 50000


# ════════════════════════════════════════════════════════
# Utilidades
# ════════════════════════════════════════════════════════

def fecha_o_none(valor) -> object:
    """
    Convierte fechas inválidas a None antes del INSERT.
    El C# original usaba '0001-01-01' como centinela de fecha vacía.
    """
    if valor is None:
        return None
    if str(valor)[:10] <= FECHA_INVALIDA:
        return None
    return valor


def limpiar_fechas(filas: list, indices: list) -> list:
    """
    Aplica fecha_o_none() a los índices indicados en cada fila.
    Disponible para operaciones futuras que lo necesiten.
    """
    indices_set = set(indices)
    return [
        tuple(fecha_o_none(v) if i in indices_set else v
              for i, v in enumerate(fila))
        for fila in filas
    ]


def _get_conn(conn_id: str):
    """
    Crea conexión mysql-connector-python usando credenciales de Airflow.
    mysql-connector-python optimiza executemany como multi-values INSERT.
    """
    c = BaseHook.get_connection(conn_id)
    return mysql.connector.connect(
        host     = c.host,
        port     = c.port or 3306,
        user     = c.login,
        password = c.password,
        database = c.schema,
        autocommit = False,
    )


# ════════════════════════════════════════════════════════
# Motor de sincronización — mismo servidor, dos cursores
# ════════════════════════════════════════════════════════

def sincronizar(
    dag_id        : str,
    conn_id       : str,
    sql_truncate  : str,
    sql_select    : str,
    sql_insert    : str,
    sql_log       : str,
    indices_fecha : list = None,
) -> int:
    """
    Sincroniza origen → destino en el mismo servidor MariaDB.
    Usa UNA conexión con DOS cursores — cur_select para leer
    y cur_insert para escribir — sin contención entre sesiones.
    Procesa en lotes de BATCH_SIZE con mysql-connector-python.
    """
    nombre = sql_select.split('/')[-1].replace('.sql', '')
    conn   = None

    try:
        conn = _get_conn(conn_id)

        cur_select = conn.cursor()
        cur_insert = conn.cursor()

        # ── Truncar destino ──────────────────────────────
        print(f"[{dag_id}] {nombre} — truncando tabla...")
        cur_insert.execute(cargar_sql(sql_truncate))

        # ── Leer origen ──────────────────────────────────
        print(f"[{dag_id}] {nombre} — leyendo origen...")
        cur_select.execute(cargar_sql(sql_select))

        # ── Insertar en lotes ────────────────────────────
        total          = 0
        sql_insert_str = cargar_sql(sql_insert)

        while True:
            filas = cur_select.fetchmany(BATCH_SIZE)
            if not filas:
                break
            if indices_fecha:
                filas = limpiar_fechas(filas, indices_fecha)
            cur_insert.executemany(sql_insert_str, filas)
            total += len(filas)

        cur_insert.execute(sql_log)
        conn.commit()
        print(f"[{dag_id}] {nombre} — OK ✅  ({total:,} filas)")
        return total

    except Exception:
        if conn:
            conn.rollback()
        print(f"[{dag_id}] {nombre} — ERROR ❌\n{traceback.format_exc()}")
        raise

    finally:
        if conn:
            conn.close()


# ════════════════════════════════════════════════════════
# Motor de sincronización — servidores distintos
# ════════════════════════════════════════════════════════

def sincronizar_entre_servidores(
    dag_id          : str,
    conn_id_origen  : str,
    conn_id_destino : str,
    sql_truncate    : str,
    sql_select      : str,
    sql_insert      : str,
    sql_log         : str,
    indices_fecha   : list = None,
) -> int:
    """
    Sincroniza origen → destino en servidores DISTINTOS MariaDB.
    Dos conexiones físicas separadas — una por servidor.
    Procesa en lotes de BATCH_SIZE con mysql-connector-python.
    """
    nombre       = sql_select.split('/')[-1].replace('.sql', '')
    conn_origen  = None
    conn_destino = None

    try:
        conn_origen  = _get_conn(conn_id_origen)
        conn_destino = _get_conn(conn_id_destino)

        cur_origen  = conn_origen.cursor()
        cur_destino = conn_destino.cursor()

        # ── Truncar destino ──────────────────────────────
        print(f"[{dag_id}] {nombre} — truncando tabla...")
        cur_destino.execute(cargar_sql(sql_truncate))

        # ── Leer origen ──────────────────────────────────
        print(f"[{dag_id}] {nombre} — leyendo origen ({conn_id_origen})...")
        cur_origen.execute(cargar_sql(sql_select))

        # ── Insertar en lotes ────────────────────────────
        total          = 0
        sql_insert_str = cargar_sql(sql_insert)

        while True:
            filas = cur_origen.fetchmany(BATCH_SIZE)
            if not filas:
                break
            if indices_fecha:
                filas = limpiar_fechas(filas, indices_fecha)
            cur_destino.executemany(sql_insert_str, filas)
            total += len(filas)

        cur_destino.execute(sql_log)
        conn_destino.commit()
        print(f"[{dag_id}] {nombre} — OK ✅  ({total:,} filas)")
        return total

    except Exception:
        if conn_destino:
            conn_destino.rollback()
        print(f"[{dag_id}] {nombre} — ERROR ❌\n{traceback.format_exc()}")
        raise

    finally:
        if conn_origen:  conn_origen.close()
        if conn_destino: conn_destino.close()
