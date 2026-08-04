# ═══════════════════════════════════════════════════════
# common/db_datasync.py
# Objetivo : Motor de sincronización MariaDB → MariaDB
#            Reutilizable para todas las operaciones de
#            etl_datasync — nadie más instancia conexiones
# Versión  : 2.1 — 2026-08-03
# Cambios  : BATCH_SIZE ajustado a 10,000
#            max_allowed_packet del servidor es 16MB
#            50,000 filas x ~500 bytes = ~25MB > 16MB límite
#            10,000 filas x ~500 bytes = ~5MB  < 16MB seguro
# ═══════════════════════════════════════════════════════
import traceback
import mysql.connector
from airflow.hooks.base import BaseHook
from common.sql_loader   import cargar_sql

FECHA_INVALIDA = '0001-01-01'
BATCH_SIZE     = 10000


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
    Conexiones dedicadas por BD evitan Commands out of sync —
    mismo patrón que el C# original (dbGusa, dbFlyBack, dbBuyBack, etc).
    """
    c = BaseHook.get_connection(conn_id)
    return mysql.connector.connect(
        host       = c.host,
        port       = c.port or 3306,
        user       = c.login,
        password   = c.password,
        database   = c.schema,
        autocommit = False,
        use_pure   = True,    # evita Commands out of sync de la extension C
    )


def _insertar_en_lotes(cursor, sql_insert: str, filas: list, indices_fecha: list = None) -> int:
    """
    Inserta filas en lotes de BATCH_SIZE.
    mysql-connector-python optimiza executemany como INSERT multi-values.
    BATCH_SIZE calibrado para respetar max_allowed_packet = 16MB.
    """
    total = 0
    for i in range(0, len(filas), BATCH_SIZE):
        lote = filas[i:i + BATCH_SIZE]
        if indices_fecha:
            lote = limpiar_fechas(lote, indices_fecha)
        cursor.executemany(sql_insert, lote)
        total += len(lote)
    return total


# ════════════════════════════════════════════════════════
# Motor de sincronización — mismo servidor
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
    Usa conexión dedicada mysql-connector-python.
    fetchall() consume todos los resultados antes de insertar.
    """
    nombre = sql_select.split('/')[-1].replace('.sql', '')
    conn   = None

    try:
        conn   = _get_conn(conn_id)
        cursor = conn.cursor()

        print(f"[{dag_id}] {nombre} — truncando tabla...")
        cursor.execute(cargar_sql(sql_truncate))

        print(f"[{dag_id}] {nombre} — leyendo origen...")
        cursor.execute(cargar_sql(sql_select))
        filas = cursor.fetchall()
        total_leidas = len(filas)
        print(f"[{dag_id}] {nombre} — {total_leidas:,} registros leídos")

        sql_insert_str = cargar_sql(sql_insert)
        total = _insertar_en_lotes(cursor, sql_insert_str, filas, indices_fecha)

        cursor.execute(sql_log)
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
    Dos conexiones dedicadas — una por BD origen, una por BD destino.
    Mismo patrón que C#: dbOrigen.Fill() → dbGlobal.Execute()
    """
    nombre       = sql_select.split('/')[-1].replace('.sql', '')
    conn_origen  = None
    conn_destino = None

    try:
        conn_origen  = _get_conn(conn_id_origen)
        conn_destino = _get_conn(conn_id_destino)

        cur_origen  = conn_origen.cursor()
        cur_destino = conn_destino.cursor()

        print(f"[{dag_id}] {nombre} — truncando tabla...")
        cur_destino.execute(cargar_sql(sql_truncate))

        print(f"[{dag_id}] {nombre} — leyendo origen ({conn_id_origen})...")
        cur_origen.execute(cargar_sql(sql_select))
        filas = cur_origen.fetchall()
        total_leidas = len(filas)
        print(f"[{dag_id}] {nombre} — {total_leidas:,} registros leídos")

        # ── Cerrar origen antes de insertar ────────────────
        # mysql-connector-python da Commands out of sync si
        # cur_origen queda abierto mientras cur_destino inserta
        cur_origen.close()
        conn_origen.close()
        conn_origen = None

        sql_insert_str = cargar_sql(sql_insert)
        total = _insertar_en_lotes(cur_destino, sql_insert_str, filas, indices_fecha)

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
