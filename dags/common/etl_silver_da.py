# ═══════════════════════════════════════════════════════
# etl_silver_da.py
# Capa    : Datos (DA)
# Objetivo: Proveer conexiones, watermarks y ejecución
#           INSERT/UPDATE Silver para clientes
# Carpeta : common/
# Versión : 1.7 — 2026-09-24
#   v1.6: Contrato de excepciones completo
#   v1.7: Separación exacta de fases escritura vs commit
#         commit_intentado = False → fallo es de escritura
#         commit_intentado = True  → fallo es SIEMPRE Unknown
#         rollback tras fallo de commit → siempre Unknown
#         rollback tras fallo de escritura → Rollback o Unknown
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
from common.sql_loader import cargar_sql

BATCH_SIZE = 1000


# ════════════════════════════════════════════════════════
# Excepciones específicas — contrato DA → NE
# ════════════════════════════════════════════════════════

class SilverPreWriteError(Exception):
    """
    Fallo antes de intentar escribir en Silver.
    Causas: fuente inválida, watermark, conexión destino,
            lectura de SQL, lectura de RAW.
    → No hubo escritura ni rollback.
    → Silver intacto.
    """

class SilverRollbackError(Exception):
    """
    Fallo durante escritura (antes del COMMIT) +
    ROLLBACK confirmado exitosamente.
    Significado exclusivo y exacto: rollback ejecutado.
    → Silver intacto.
    """

class SilverTransactionUnknownError(Exception):
    """
    Estado de la transacción desconocido. Causas posibles:
      - COMMIT lanzó excepción (no sabemos si se completó)
      - Rollback falló tras fallo de escritura
      - Rollback falló tras fallo de commit
    → Estado de Silver indeterminado — revisar manualmente.
    """


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
  , 'fi': {
        'select_insert' : 'sql/clients/select_silver_clientsfi_insert.sql'
      , 'select_update' : 'sql/clients/select_silver_clientsfi_update.sql'
      , 'insert'        : 'sql/clients/insert_silver_clientsfi.sql'
      , 'update'        : 'sql/clients/update_silver_clientsfi.sql'
      , 'tabla_destino' : 'source.clientsfi'
      , 'mariadb_conn'  : ORIGEN_CONN_ID_240
      , 'raw_tabla'     : 'db_general.tbl_raw_clientsfi'
    }
  , 'vc': {
        'select_insert' : 'sql/clients/select_silver_clientsvc_insert.sql'
      , 'select_update' : 'sql/clients/select_silver_clientsvc_update.sql'
      , 'insert'        : 'sql/clients/insert_silver_clientsvc.sql'
      , 'update'        : 'sql/clients/update_silver_clientsvc.sql'
      , 'tabla_destino' : 'source.clientsvc'
      , 'mariadb_conn'  : ORIGEN_CONN_ID_240
      , 'raw_tabla'     : 'db_general.tbl_raw_clientsvc'
    }
}

_LOG = {
    '242': LOG_PATH
  , '240': LOG_PATH
}


# ════════════════════════════════════════════════════════
# Conexiones — uso interno
# ════════════════════════════════════════════════════════

def _get_pyodbc_conn():
    """Conexión pyodbc a SQL Server. autocommit=False → transacción."""
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


def _cerrar_conn(conn, nombre: str) -> None:
    """
    Cierra una conexión de forma segura.
    Un fallo de close() se registra como WARNING
    y nunca reemplaza el resultado de la transacción.
    """
    if conn is None:
        return
    try:
        conn.close()
    except Exception as e:
        print(f"[DA] WARNING: fallo al cerrar {nombre}: {e}")


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
        _cerrar_conn(conn, 'pyodbc-watermark-id')


def get_max_updatedat_destino(tabla_destino: str) -> datetime:
    """MAX(updatedAt) en SQL Server destino. NULL → 2000-01-01."""
    conn = None
    try:
        conn   = _get_pyodbc_conn()
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT ISNULL(MAX(updatedAt), '2000-01-01') FROM {tabla_destino}"
        )
        resultado = cursor.fetchone()[0]
        return resultado if resultado else datetime(2000, 1, 1)
    finally:
        _cerrar_conn(conn, 'pyodbc-watermark-upd')


def get_min_updatedat_raw(conn_id: str, raw_tabla: str) -> datetime:
    """MIN(updatedAt) en RAW MariaDB — fecha de nacimiento de la RAW."""
    conn = None
    try:
        conn   = _get_mariadb_raw_conn(conn_id)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT MIN(updatedAt) FROM {raw_tabla} WHERE deletedAt IS NULL"
        )
        return cursor.fetchone()[0]
    finally:
        _cerrar_conn(conn, 'mariadb-watermark-raw')


def detectar_modo(tabla_destino: str, conn_id: str, raw_tabla: str) -> str:
    """
    Detecta automáticamente modo full o incremental.
    Lógica idempotente basada en MIN(updatedAt RAW).
    Returns: 'full' o 'incremental'
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
# Ejecución Silver — UPDATE + INSERT en una transacción
# ════════════════════════════════════════════════════════

def ejecutar_silver(fuente: str) -> tuple:
    """
    Ejecuta UPDATE + INSERT Silver en UNA SOLA TRANSACCIÓN.

    Contrato de excepciones (exacto):

      SilverPreWriteError
        → fallo antes de escribir (fuente inválida, watermark,
          apertura de conn_destino, lectura de SQL o RAW)
        → Silver intacto, sin rollback

      SilverRollbackError
        → fallo durante escritura (UPDATE o INSERT), ANTES del COMMIT
        → rollback ejecutado exitosamente
        → Silver intacto

      SilverTransactionUnknownError
        → fallo del COMMIT (no sabemos si se completó)
        → O rollback falló tras fallo de escritura o de commit
        → Estado de Silver indeterminado — revisar manualmente

      (éxito) → retorna (filas_update, filas_insert, modo, segundos)
    """

    # ── FASE 1: Pre-escritura ─────────────────────────────
    # Todo lo que puede fallar antes de abrir conn_destino.
    # Cualquier fallo → SilverPreWriteError.
    # ─────────────────────────────────────────────────────
    try:
        cfg = _SQL[fuente]   # ← KeyError clasificado aquí

        max_id        = get_max_id_destino(cfg['tabla_destino'])
        max_updatedat = get_max_updatedat_destino(cfg['tabla_destino'])
        modo          = detectar_modo(
                            cfg['tabla_destino']
                          , cfg['mariadb_conn']
                          , cfg['raw_tabla']
                        )

        if modo == 'full':
            query_select_update = cargar_sql(
                cfg['select_update']
              , max_id        = max_id
              , max_updatedat = "'2000-01-01'"
            )
        else:
            query_select_update = cargar_sql(
                cfg['select_update']
              , max_id        = max_id
              , max_updatedat = f"'{max_updatedat}'"
            )

        query_select_insert = cargar_sql(cfg['select_insert'], max_id=max_id)
        query_update        = cargar_sql(cfg['update'])
        query_insert        = cargar_sql(cfg['insert'])

    except Exception as e:
        raise SilverPreWriteError(
            f"Fallo antes de escribir en Silver [{fuente}]: {e}"
        ) from e

    # ── FASE 2: Apertura de conexión destino ─────────────
    # Separada de FASE 1 para clasificar correctamente.
    # Fallo aquí → SilverPreWriteError (no hubo escritura).
    # ─────────────────────────────────────────────────────
    conn_origen  = None
    conn_destino = None

    try:
        conn_origen  = _get_mariadb_conn(cfg['mariadb_conn'])
        conn_destino = _get_pyodbc_conn()   # autocommit=False
    except Exception as e:
        # conn_destino no se abrió → no hubo escritura
        _cerrar_conn(conn_origen,  f'mariadb-apertura-{fuente}')
        _cerrar_conn(conn_destino, f'pyodbc-apertura-{fuente}')
        raise SilverPreWriteError(
            f"Fallo al abrir conexiones [{fuente}]: {e}"
        ) from e

    # ── FASE 3: Escritura (UPDATE + INSERT) ──────────────
    # Bandera commit_intentado distingue fase escritura
    # de fase commit para clasificar el error correctamente.
    # ─────────────────────────────────────────────────────
    filas_update      = 0
    filas_insert      = 0
    inicio            = time.time()
    commit_intentado  = False   # ← clave para clasificar el fallo

    try:
        cursor_origen                   = conn_origen.cursor()
        cursor_destino                  = conn_destino.cursor()
        cursor_destino.fast_executemany = True

        # ── UPDATE ───────────────────────────────────────
        print(f"[Silver {fuente}] UPDATE modo={modo} iniciando...")
        cursor_origen.execute(query_select_update)

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
            filas_update += len(lote_update)

        print(f"[Silver {fuente}] UPDATE {filas_update:,} — pendiente COMMIT")

        # ── INSERT ───────────────────────────────────────
        print(f"[Silver {fuente}] INSERT iniciando...")
        cursor_origen.execute(query_select_insert)

        while True:
            lote = cursor_origen.fetchmany(BATCH_SIZE)
            if not lote:
                break
            cursor_destino.executemany(query_insert, lote)
            filas_insert += len(lote)

        print(f"[Silver {fuente}] INSERT {filas_insert:,} — pendiente COMMIT")

        # ── COMMIT ───────────────────────────────────────
        # A partir de aquí cualquier fallo → Unknown
        commit_intentado = True
        conn_destino.commit()
        # commit() retornó → COMMIT confirmado
        segundos = time.time() - inicio

    except Exception as e_escritura:
        # ── Clasificar según la fase en que falló ────────
        if commit_intentado:
            # COMMIT lanzó excepción → estado incierto
            # aunque rollback posterior tenga éxito
            _intentar_rollback_unknown(conn_destino, fuente, e_escritura)
        else:
            # Fallo en escritura (UPDATE o INSERT), antes del COMMIT
            # → rollback determina si es Rollback o Unknown
            _intentar_rollback_escritura(conn_destino, fuente, e_escritura)

    finally:
        _cerrar_conn(conn_origen,  f'mariadb-escritura-{fuente}')
        _cerrar_conn(conn_destino, f'pyodbc-escritura-{fuente}')

    # ── Post-COMMIT: print separado y protegido ──────────
    # Fuera del try transaccional — un fallo aquí no puede
    # confundirse con un fallo de escritura ni de commit.
    try:
        print(
            f"[Silver {fuente}] COMMIT ✅ "
            f"UPDATE={filas_update:,} INSERT={filas_insert:,} "
            f"{segundos:.1f}s"
        )
    except Exception:
        pass

    return filas_update, filas_insert, modo, segundos


# ════════════════════════════════════════════════════════
# Helpers de rollback — uso interno
# ════════════════════════════════════════════════════════

def _intentar_rollback_escritura(conn, fuente: str, causa: Exception) -> None:
    """
    Rollback tras fallo de escritura (antes del COMMIT).
    Rollback OK  → SilverRollbackError   (estado conocido)
    Rollback falla → SilverTransactionUnknownError
    """
    try:
        conn.rollback()
        print(f"[Silver {fuente}] ROLLBACK ✅ — Silver sin cambios")
        raise SilverRollbackError(
            f"ROLLBACK confirmado [{fuente}] | causa: {causa}"
        ) from causa
    except SilverRollbackError:
        raise
    except Exception as e_rb:
        raise SilverTransactionUnknownError(
            f"Rollback falló [{fuente}] — Silver en estado desconocido "
            f"| causa escritura: {causa} "
            f"| causa rollback: {e_rb}"
        ) from causa


def _intentar_rollback_unknown(conn, fuente: str, causa: Exception) -> None:
    """
    Rollback tras fallo del COMMIT.
    El COMMIT puede haberse completado aunque haya lanzado excepción.
    → Siempre SilverTransactionUnknownError,
      independientemente del resultado del rollback.
    """
    try:
        conn.rollback()
        print(f"[Silver {fuente}] WARNING: rollback post-commit — estado incierto")
    except Exception as e_rb:
        print(f"[Silver {fuente}] WARNING: rollback post-commit también falló: {e_rb}")

    raise SilverTransactionUnknownError(
        f"COMMIT falló [{fuente}] — estado de Silver desconocido "
        f"| causa commit: {causa}"
    ) from causa


def get_log_path(instancia: str) -> str:
    """Devuelve la ruta del log para la instancia."""
    return _LOG[instancia]
