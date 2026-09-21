# ═══════════════════════════════════════════════════════
# etl_silver_da.py
# Capa    : Datos (DA)
# Objetivo: Proveer conexiones, watermarks y ejecución
#           INSERT/UPDATE Silver para clientes
# Carpeta : common/
# Versión : 1.1 — 2026-09-21
#   v1.0: fb solamente
#   v1.1: agregado bb y ml (instancia 242)
# ═══════════════════════════════════════════════════════
import time
import pyodbc
import mysql.connector
from datetime              import datetime
from airflow.hooks.base    import BaseHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from common.db_connections import (
    ORIGEN_CONN_ID_242
  , ORIGEN_CONN_ID_240
  , MSSQL_CONN_ID
  , LOG_PATH
)
from common.sql_loader     import cargar_sql
from common.error_classifier import generar_reporte_error, generar_reporte_success

BATCH_SIZE = 1000

# ── Rutas SQL externas ───────────────────────────────────
_SQL = {
    'fb': {
        'select_insert' : 'sql/clients/select_silver_clientsfb_insert.sql'
      , 'select_update' : 'sql/clients/select_silver_clientsfb_update.sql'
      , 'insert'        : 'sql/clients/insert_silver_clientsfb.sql'
      , 'update'        : 'sql/clients/update_silver_clientsfb.sql'
      , 'tabla_destino' : 'source.clientsfb'
      , 'mariadb_conn'  : ORIGEN_CONN_ID_242
      , 'raw_tabla'     : 'db_general.tbl_raw_clientsfb'
    }
  , 'bb': {
        'select_insert' : 'sql/clients/select_silver_clientsbb_insert.sql'
      , 'select_update' : 'sql/clients/select_silver_clientsbb_update.sql'
      , 'insert'        : 'sql/clients/insert_silver_clientsbb.sql'
      , 'update'        : 'sql/clients/update_silver_clientsbb.sql'
      , 'tabla_destino' : 'source.clientsbb'
      , 'mariadb_conn'  : ORIGEN_CONN_ID_242
      , 'raw_tabla'     : 'db_general.tbl_raw_clientsbb'
    }
  , 'ml': {
        'select_insert' : 'sql/clients/select_silver_clientsml_insert.sql'
      , 'select_update' : 'sql/clients/select_silver_clientsml_update.sql'
      , 'insert'        : 'sql/clients/insert_silver_clientsml.sql'
      , 'update'        : 'sql/clients/update_silver_clientsml.sql'
      , 'tabla_destino' : 'source.clientsml'
      , 'mariadb_conn'  : ORIGEN_CONN_ID_242
      , 'raw_tabla'     : 'db_general.tbl_raw_clientsml'
    }
    # ── fi, vc (instancia 240) se agregan aquí después ───
}

# ── Mapa de log por instancia ────────────────────────────
_LOG = {
    '242': LOG_PATH
  , '240': LOG_PATH
}


# ════════════════════════════════════════════════════════
# Conexiones — uso interno
# ════════════════════════════════════════════════════════

def _get_pyodbc_conn():
    """Conexión pyodbc a SQL Server — igual que etl_base."""
    c = BaseHook.get_connection(MSSQL_CONN_ID)
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={c.host};"
        f"DATABASE={c.schema};"
        f"UID={c.login};"
        f"PWD={c.password};"
        f"TrustServerCertificate=yes;"
    )
    conn = pyodbc.connect(conn_str)
    conn.autocommit = False
    return conn


def _get_mariadb_conn(conn_id: str):
    """Conexión MariaDB via MySqlHook."""
    hook = MySqlHook(mysql_conn_id=conn_id)
    return hook.get_conn()


def _get_mariadb_raw_conn(conn_id: str):
    """Conexión MariaDB via mysql-connector para watermarks."""
    c = BaseHook.get_connection(conn_id)
    return mysql.connector.connect(
        host     = c.host
      , port     = c.port or 3306
      , user     = c.login
      , password = c.password
      , database = c.schema
      , use_pure = True
    )


# ════════════════════════════════════════════════════════
# Watermarks
# ════════════════════════════════════════════════════════

def get_max_id_destino(tabla_destino: str) -> int:
    """MAX(clientid) en SQL Server destino."""
    conn = None
    try:
        conn   = _get_pyodbc_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT ISNULL(MAX(clientid), 0) FROM {tabla_destino}")
        return cursor.fetchone()[0]
    finally:
        if conn: conn.close()


def get_max_updatedat_destino(tabla_destino: str) -> datetime:
    """MAX(updatedAt) en SQL Server destino."""
    conn = None
    try:
        conn   = _get_pyodbc_conn()
        cursor = conn.cursor()
        cursor.execute(f"SELECT ISNULL(MAX(updatedAt), '2000-01-01') FROM {tabla_destino}")
        return cursor.fetchone()[0]
    finally:
        if conn: conn.close()


def get_min_updatedat_raw(conn_id: str, raw_tabla: str) -> datetime:
    """
    MIN(updatedAt) en RAW MariaDB.
    Representa la fecha de nacimiento de la RAW.
    Usado para detectar automáticamente modo full vs incremental.
    """
    conn = None
    try:
        conn   = _get_mariadb_raw_conn(conn_id)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT MIN(updatedAt) FROM {raw_tabla} WHERE deletedAt IS NULL"
        )
        return cursor.fetchone()[0]
    finally:
        if conn: conn.close()


def detectar_modo(tabla_destino: str, conn_id: str, raw_tabla: str) -> str:
    """
    Detecta automáticamente si debe correr en modo full o incremental.

    Lógica idempotente:
      Si MAX(updatedAt destino) < MIN(updatedAt RAW)
        → destino desactualizado vs RAW → FULL
      Si no
        → destino al día → INCREMENTAL

    Returns:
        'full' o 'incremental'
    """
    max_dest = get_max_updatedat_destino(tabla_destino)
    min_raw  = get_min_updatedat_raw(conn_id, raw_tabla)

    if hasattr(max_dest, 'tzinfo') and max_dest.tzinfo:
        max_dest = max_dest.replace(tzinfo=None)
    if hasattr(min_raw, 'tzinfo') and min_raw.tzinfo:
        min_raw = min_raw.replace(tzinfo=None)

    modo = 'full' if max_dest < min_raw else 'incremental'
    print(f"[Silver] max_dest={max_dest} | min_raw={min_raw} | modo={modo}")
    return modo


# ════════════════════════════════════════════════════════
# Ejecución INSERT Silver
# ════════════════════════════════════════════════════════

def ejecutar_insert_silver(fuente: str, dag_id: str) -> tuple:
    """
    INSERT Silver — registros nuevos (clientid > max_id destino).
    NO maneja errores — lanza excepción si falla.

    Returns:
        (filas_insertadas, reporte)
    """
    cfg          = _SQL[fuente]
    max_id       = get_max_id_destino(cfg['tabla_destino'])
    conn_origen  = None
    conn_destino = None
    filas        = 0
    inicio       = time.time()

    query_select = cargar_sql(cfg['select_insert'], max_id=max_id)
    query_insert = cargar_sql(cfg['insert'])

    try:
        conn_origen  = _get_mariadb_conn(cfg['mariadb_conn'])
        conn_destino = _get_pyodbc_conn()

        cursor_origen                   = conn_origen.cursor()
        cursor_destino                  = conn_destino.cursor()
        cursor_destino.fast_executemany = True

        cursor_origen.execute(query_select)

        while True:
            lote = cursor_origen.fetchmany(BATCH_SIZE)
            if not lote:
                break
            cursor_destino.executemany(query_insert, lote)
            conn_destino.commit()
            filas += len(lote)

        segundos = time.time() - inicio
        reporte  = generar_reporte_success(
            dag_id        = dag_id
          , vista_origen  = cfg['raw_tabla']
          , tabla_destino = cfg['tabla_destino']
          , max_id        = max_id
          , filas_ok      = filas
          , segundos      = segundos
        )
        print(f"[Silver INSERT {fuente}] OK | {filas:,} filas | {segundos:.1f}s")
        return filas, reporte

    finally:
        if conn_origen  : conn_origen.close()
        if conn_destino : conn_destino.close()


# ════════════════════════════════════════════════════════
# Ejecución UPDATE Silver
# ════════════════════════════════════════════════════════

def ejecutar_update_silver(fuente: str, dag_id: str) -> tuple:
    """
    UPDATE Silver — registros modificados.
    Detecta automáticamente modo full o incremental.
    NO maneja errores — lanza excepción si falla.

    MODO FULL        : updatedAt > '2000-01-01' → trae todos
                       → primera vez o reset de tabla
    MODO INCREMENTAL : updatedAt > max_updatedAt_destino
                       → solo los cambiados desde última carga

    Returns:
        (filas_actualizadas, reporte, modo)
    """
    cfg           = _SQL[fuente]
    max_id        = get_max_id_destino(cfg['tabla_destino'])
    max_updatedat = get_max_updatedat_destino(cfg['tabla_destino'])
    modo          = detectar_modo(
                        cfg['tabla_destino']
                      , cfg['mariadb_conn']
                      , cfg['raw_tabla']
                    )

    conn_origen  = None
    conn_destino = None
    filas        = 0
    inicio       = time.time()

    if modo == 'full':
        query_select = cargar_sql(
            cfg['select_update']
          , max_id        = max_id
          , max_updatedat = "'2000-01-01'"
        )
    else:
        query_select = cargar_sql(
            cfg['select_update']
          , max_id        = max_id
          , max_updatedat = f"'{max_updatedat}'"
        )

    query_update = cargar_sql(cfg['update'])

    try:
        conn_origen  = _get_mariadb_conn(cfg['mariadb_conn'])
        conn_destino = _get_pyodbc_conn()

        cursor_origen                   = conn_origen.cursor()
        cursor_destino                  = conn_destino.cursor()
        cursor_destino.fast_executemany = True

        cursor_origen.execute(query_select)

        while True:
            lote = cursor_origen.fetchmany(BATCH_SIZE)
            if not lote:
                break

            lote_update = []
            for fila in lote:
                lote_update.append((
                    fila[0]   # productid
                  , fila[1]   # contractid
                  , fila[3]   # email
                  , fila[4]   # capdata
                  , fila[5]   # FirstName
                  , fila[6]   # LastName
                  , fila[7]   # countrycode
                  , fila[8]   # country
                  , fila[9]   # Estate
                  , fila[10]  # ciudad
                  , fila[11]  # address
                  , fila[12]  # zip
                  , fila[13]  # corpcode
                  , fila[14]  # corp
                  , fila[15]  # ingreso
                  , fila[16]  # egreso
                  , fila[17]  # rank
                  , fila[18]  # EstatusN
                  , fila[19]  # EstatusL
                  , fila[21]  # updatedAt
                  , fila[22]  # deletedAt
                  , fila[2]   # clientid ← WHERE al final
                ))

            cursor_destino.executemany(query_update, lote_update)
            conn_destino.commit()
            filas += len(lote_update)

        segundos = time.time() - inicio
        reporte  = generar_reporte_success(
            dag_id        = dag_id
          , vista_origen  = cfg['raw_tabla']
          , tabla_destino = cfg['tabla_destino']
          , max_id        = max_id
          , filas_ok      = filas
          , segundos      = segundos
        )
        print(f"[Silver UPDATE {fuente}] modo={modo} | {filas:,} filas | {segundos:.1f}s")
        return filas, reporte, modo

    finally:
        if conn_origen  : conn_origen.close()
        if conn_destino : conn_destino.close()


def get_log_path(instancia: str) -> str:
    """Devuelve la ruta del log para la instancia."""
    return _LOG[instancia]
