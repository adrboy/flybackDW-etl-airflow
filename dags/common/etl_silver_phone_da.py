# ═══════════════════════════════════════════════════════
# etl_silver_phone_da.py
# Capa    : Datos (DA)
# Objetivo: Silver incremental de teléfonos
#           RAW MariaDB (tbl_raw_phoneXX) → SQL Server (source.PhoneXX)
#           UPDATE + INSERT por conjuntos en UNA transacción,
#           con candado por fuente y auditoría en etl_audit_log
# Carpeta : common/
# Versión : 1.0 — 2026-11-09 — BORRADOR pendiente de pruebas
#           Habilitadas: las cinco — fb, bb, ml, fi, vc (FUENTES_HABILITADAS)
# Molde   : etl_silver_da.py v1.7 — se reutilizan sus excepciones,
#           su conexión a SQL Server, su marca de agua y sus helpers
#           de rollback, para que el contrato de errores sea idéntico.
#
# ── Tres conexiones ─────────────────────────────────────
#   control  (MariaDB, autocommit) : candado + filas de auditoría.
#            Se confirma al instante: aunque falle SQL Server,
#            el error queda anotado en etl_audit_log de la instancia.
#   origen   (MariaDB, lectura)    : lee tbl_raw_phoneXX
#   destino  (SQL Server, SIN autocommit) : #delta → UPDATE → INSERT
#
# ── Una sola ejecución a la vez ─────────────────────────
#   GET_LOCK('silver_phone_<fuente>') sobre la conexión de control,
#   espera hasta LOCK_TIMEOUT_SEG. Si el proceso muere, la base de
#   datos libera el candado sola al cerrarse la conexión.
#   (max_active_runs=1 en el DAG cubre el caso normal de Airflow;
#    el candado cubre además un disparo manual o tasks test.)
#
# ── Idempotencia ────────────────────────────────────────
#   marca = MAX(updatedAt) de Silver, leída con >=; el UPDATE solo
#   toca filas que difieren y el INSERT solo agrega lo que falta.
#   Segunda ejecución seguida = 0 y 0.
# ═══════════════════════════════════════════════════════
import time
import mysql.connector
import pyodbc
from datetime              import datetime
from airflow.hooks.base    import BaseHook

from common.db_connections import ORIGEN_CONN_ID_242, ORIGEN_CONN_ID_240
from common.sql_loader     import cargar_sql
from common.etl_silver_da  import (
    SilverPreWriteError
  , SilverRollbackError
  , SilverTransactionUnknownError
  , get_max_updatedat_destino
  , get_log_path
  , _get_pyodbc_conn
  , _get_mariadb_conn
  , _cerrar_conn
  , _intentar_rollback_escritura
  , _intentar_rollback_unknown
)

DAG_ID           = 'dag_phones_quincenal'
BATCH_SIZE       = 5000
LOCK_TIMEOUT_SEG = 300

# Solo las fuentes cuya tabla Silver ya se recreó con la estructura nueva
# (clientid, phone, createdAt, updatedAt, deletedAt + índice único).
# Las cinco fuentes se habilitaron una por una, tras probar carga e idempotencia.
FUENTES_HABILITADAS = {'fb', 'bb', 'ml', 'fi', 'vc'}

# ── Rutas SQL externas (genéricas: sirven para los 5 productos) ──
_SQL_SELECT       = 'sql/phones/select_silver_phone_delta.sql'
_SQL_CREATE_DELTA = 'sql/phones/create_silver_phone_delta.sql'
_SQL_INSERT_DELTA = 'sql/phones/insert_silver_phone_delta.sql'
_SQL_UPDATE       = 'sql/phones/update_silver_phone.sql'
_SQL_INSERT       = 'sql/phones/insert_silver_phone.sql'

# ── Configuración por producto ───────────────────────────
_PHONE = {
    'fb': {
        'mariadb_conn' : ORIGEN_CONN_ID_242
      , 'raw_tabla'    : 'db_general.tbl_raw_phonefb'
      , 'tabla_destino': 'source.Phonefb'
    }
  , 'bb': {
        'mariadb_conn' : ORIGEN_CONN_ID_242
      , 'raw_tabla'    : 'db_general.tbl_raw_phonebb'
      , 'tabla_destino': 'source.Phonebb'
    }
  , 'ml': {
        'mariadb_conn' : ORIGEN_CONN_ID_242
      , 'raw_tabla'    : 'db_general.tbl_raw_phoneml'
      , 'tabla_destino': 'source.Phoneml'
    }
  , 'fi': {
        'mariadb_conn' : ORIGEN_CONN_ID_240
      , 'raw_tabla'    : 'db_general.tbl_raw_phonefi'
      , 'tabla_destino': 'source.Phonefi'
    }
  , 'vc': {
        'mariadb_conn' : ORIGEN_CONN_ID_240
      , 'raw_tabla'    : 'db_general.tbl_raw_phonevc'
      , 'tabla_destino': 'source.Phonevc'
    }
}

# Tipos explícitos para executemany con fast_executemany:
# evita el error de pyodbc cuando el primer valor de una columna es None.
_TIPOS_DELTA = [
    (pyodbc.SQL_INTEGER, 0,  0)   # clientid
  , (pyodbc.SQL_VARCHAR, 30, 0)   # phone
  , (pyodbc.SQL_VARCHAR, 30, 0)   # updatedAt (texto)
  , (pyodbc.SQL_VARCHAR, 30, 0)   # deletedAt (texto o NULL)
]


# ════════════════════════════════════════════════════════
# Utilidades — uso interno
# ════════════════════════════════════════════════════════

def _fmt_fecha(valor):
    """datetime → 'YYYY-MM-DD HH:MM:SS' (o None)."""
    if valor is None:
        return None
    return valor.strftime('%Y-%m-%d %H:%M:%S')


def _get_control_conn(conn_id: str):
    """
    Conexión de control: candado + auditoría.
    autocommit=True → cada fila de auditoría se confirma al instante,
    independiente de lo que pase con la transacción de SQL Server.
    """
    c = BaseHook.get_connection(conn_id)
    return mysql.connector.connect(
        host       = c.host
      , port       = c.port or 3306
      , user       = c.login
      , password   = c.password
      , database   = c.schema
      , use_pure   = True
      , autocommit = True
    )


# ════════════════════════════════════════════════════════
# Candado — una sola ejecución por fuente
# ════════════════════════════════════════════════════════

def _obtener_candado(conn_ctl, fuente: str) -> None:
    """
    GET_LOCK sobre la conexión de control. Espera hasta LOCK_TIMEOUT_SEG.
    Lanza RuntimeError si no lo consigue (otra ejecución sigue corriendo).
    """
    cursor = conn_ctl.cursor()
    cursor.execute("SELECT GET_LOCK(%s, %s)", (f"silver_phone_{fuente}", LOCK_TIMEOUT_SEG))
    fila = cursor.fetchone()
    cursor.close()
    if not fila or fila[0] != 1:
        raise RuntimeError(
            f"ya hay una ejecución de Silver teléfonos {fuente} en curso "
            f"(esperó {LOCK_TIMEOUT_SEG}s)"
        )


def _liberar_candado(conn_ctl, fuente: str) -> None:
    """Libera el candado. Un fallo aquí es solo WARNING."""
    try:
        cursor = conn_ctl.cursor()
        cursor.execute("SELECT RELEASE_LOCK(%s)", (f"silver_phone_{fuente}",))
        cursor.fetchone()
        cursor.close()
    except Exception as e:
        print(f"[DA-phone] WARNING: no se pudo liberar el candado {fuente}: {e}")


# ════════════════════════════════════════════════════════
# Auditoría — etl_audit_log de la instancia (conexión de control)
# Nunca bloquea ni altera el resultado de la transacción de datos
# ════════════════════════════════════════════════════════

def _auditoria_inicio(conn_ctl, cfg: dict) -> dict:
    """
    Inserta dos filas RUNNING: SILVER-INSERT y SILVER-UPDATE.
    Devuelve {tipo: id}. Si falla, devuelve {} y solo avisa.
    """
    ids = {}
    try:
        ahora  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor = conn_ctl.cursor()
        for tipo in ('SILVER-INSERT', 'SILVER-UPDATE'):
            cursor.execute(
                "INSERT INTO db_general.etl_audit_log "
                "(paquete, vista_origen, tabla_destino, tipo_ejecucion, estado, fecha_inicio) "
                "VALUES (%s, %s, %s, %s, %s, %s)"
              , (DAG_ID, cfg['raw_tabla'], cfg['tabla_destino'], tipo, 'RUNNING', ahora)
            )
            ids[tipo] = cursor.lastrowid
        cursor.close()
    except Exception as e:
        print(f"[DA-phone] WARNING: auditoría de inicio no registrada: {e}")
    return ids


def _auditoria_cierre(conn_ctl, ids: dict, estado: str,
                      filas_insert, filas_update, mensaje) -> None:
    """
    Cierra las filas de auditoría: OK, ERROR o UNKNOWN.
    filas_insertadas guarda el conteo de cada operación.
    """
    if not ids or conn_ctl is None:
        return
    try:
        try:
            conn_ctl.ping(reconnect=True, attempts=2, delay=1)
        except Exception:
            pass
        ahora  = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msg    = mensaje[:500] if mensaje else None
        cursor = conn_ctl.cursor()
        for tipo, filas in (('SILVER-INSERT', filas_insert), ('SILVER-UPDATE', filas_update)):
            if ids.get(tipo):
                cursor.execute(
                    "UPDATE db_general.etl_audit_log "
                    "SET estado = %s, filas_insertadas = %s, mensaje_error = %s, fecha_fin = %s "
                    "WHERE id = %s"
                  , (estado, filas, msg, ahora, ids[tipo])
                )
        cursor.close()
    except Exception as e:
        print(f"[DA-phone] WARNING: auditoría de cierre no registrada: {e}")


# ════════════════════════════════════════════════════════
# Transacción Silver — UPDATE + INSERT por conjuntos
# ════════════════════════════════════════════════════════

def _transaccion_silver(fuente: str, cfg: dict) -> tuple:
    """
    Fases 1 a 3 del contrato de Silver de clientes.

    Contrato de excepciones (idéntico a ejecutar_silver):
      SilverPreWriteError           → fallo antes de escribir; Silver intacto
      SilverRollbackError           → fallo durante la escritura; rollback OK
      SilverTransactionUnknownError → COMMIT incierto o rollback fallido

    Returns:
        (filas_leidas, filas_update, filas_insert, segundos, marca)
    """

    # ── FASE 1: Pre-escritura ─────────────────────────────
    try:
        marca     = get_max_updatedat_destino(cfg['tabla_destino'])
        marca_txt = _fmt_fecha(marca)

        query_select  = cargar_sql(_SQL_SELECT
                                 , raw_tabla = cfg['raw_tabla']
                                 , marca     = f"'{marca_txt}'")
        sql_create    = cargar_sql(_SQL_CREATE_DELTA)
        sql_ins_delta = cargar_sql(_SQL_INSERT_DELTA)
        sql_update    = cargar_sql(_SQL_UPDATE, tabla_destino=cfg['tabla_destino'])
        sql_insert    = cargar_sql(_SQL_INSERT, tabla_destino=cfg['tabla_destino'])

    except Exception as e:
        raise SilverPreWriteError(
            f"Fallo antes de escribir en Silver teléfonos [{fuente}]: {e}"
        ) from e

    # ── FASE 2: Apertura de conexiones ───────────────────
    conn_origen  = None
    conn_destino = None

    try:
        conn_origen  = _get_mariadb_conn(cfg['mariadb_conn'])
        conn_destino = _get_pyodbc_conn()   # autocommit=False
    except Exception as e:
        _cerrar_conn(conn_origen,  f'mariadb-apertura-phone-{fuente}')
        _cerrar_conn(conn_destino, f'pyodbc-apertura-phone-{fuente}')
        raise SilverPreWriteError(
            f"Fallo al abrir conexiones teléfonos [{fuente}]: {e}"
        ) from e

    # ── FASE 3: Escritura ─────────────────────────────────
    filas_leidas     = 0
    filas_update     = 0
    filas_insert     = 0
    inicio           = time.time()
    commit_intentado = False

    try:
        cursor_origen  = conn_origen.cursor()
        cursor_destino = conn_destino.cursor()

        # ── #delta: lo que cambió en RAW desde la marca ───
        print(f"[Silver-phone {fuente}] leyendo RAW desde {marca_txt}...")
        cursor_destino.execute(sql_create)
        cursor_destino.fast_executemany = True
        cursor_origen.execute(query_select)

        while True:
            lote = cursor_origen.fetchmany(BATCH_SIZE)
            if not lote:
                break
            parametros = [
                (int(f[0]), f[1], _fmt_fecha(f[2]), _fmt_fecha(f[3]))
                for f in lote
            ]
            cursor_destino.setinputsizes(_TIPOS_DELTA)
            cursor_destino.executemany(sql_ins_delta, parametros)
            filas_leidas += len(parametros)

        cursor_destino.fast_executemany = False
        print(f"[Silver-phone {fuente}] RAW → #delta: {filas_leidas:,} filas")

        # ── UPDATE por conjuntos ─────────────────────────
        cursor_destino.execute(sql_update)
        filas_update = max(cursor_destino.rowcount, 0)
        print(f"[Silver-phone {fuente}] UPDATE {filas_update:,} — pendiente COMMIT")

        # ── INSERT por conjuntos ─────────────────────────
        cursor_destino.execute(sql_insert)
        filas_insert = max(cursor_destino.rowcount, 0)
        print(f"[Silver-phone {fuente}] INSERT {filas_insert:,} — pendiente COMMIT")

        # ── COMMIT ───────────────────────────────────────
        # A partir de aquí cualquier fallo → Unknown
        commit_intentado = True
        conn_destino.commit()
        segundos = time.time() - inicio

    except Exception as e_escritura:
        if commit_intentado:
            _intentar_rollback_unknown(conn_destino, fuente, e_escritura)
        else:
            _intentar_rollback_escritura(conn_destino, fuente, e_escritura)

    finally:
        _cerrar_conn(conn_origen,  f'mariadb-escritura-phone-{fuente}')
        _cerrar_conn(conn_destino, f'pyodbc-escritura-phone-{fuente}')

    try:
        print(
            f"[Silver-phone {fuente}] COMMIT ✅ "
            f"leídas={filas_leidas:,} UPDATE={filas_update:,} INSERT={filas_insert:,} "
            f"{segundos:.1f}s"
        )
    except Exception:
        pass

    return filas_leidas, filas_update, filas_insert, segundos, marca_txt


# ════════════════════════════════════════════════════════
# Función principal — candado + auditoría + transacción
# ════════════════════════════════════════════════════════

def ejecutar_silver_phone(fuente: str) -> tuple:
    """
    Silver incremental de teléfonos para una fuente.

    Orden: candado → auditoría RUNNING → transacción → auditoría de cierre
           → liberar candado (siempre, en finally).

    Contrato de excepciones: el mismo de ejecutar_silver (clientes).
    Una ejecución concurrente de la misma fuente falla con
    SilverPreWriteError (candado ocupado) y el DAG la reintenta.

    Returns:
        (filas_update, filas_insert, filas_leidas, segundos, marca)
    """
    conn_ctl  = None
    ids       = {}
    candado   = False

    try:
        # ── FASE 0: validación, control y candado ────────
        try:
            if fuente not in FUENTES_HABILITADAS:
                raise ValueError(
                    f"fuente '{fuente}' no habilitada para Silver de teléfonos "
                    f"(habilitadas: {sorted(FUENTES_HABILITADAS)})"
                )
            cfg      = _PHONE[fuente]
            conn_ctl = _get_control_conn(cfg['mariadb_conn'])
            _obtener_candado(conn_ctl, fuente)
            candado  = True
        except Exception as e:
            raise SilverPreWriteError(
                f"Control/candado Silver teléfonos [{fuente}]: {e}"
            ) from e

        ids = _auditoria_inicio(conn_ctl, cfg)

        # ── FASES 1 a 3: transacción ─────────────────────
        leidas, f_update, f_insert, segundos, marca = _transaccion_silver(fuente, cfg)

        _auditoria_cierre(conn_ctl, ids, 'OK', f_insert, f_update, None)
        return f_update, f_insert, leidas, segundos, marca

    except SilverTransactionUnknownError as e:
        _auditoria_cierre(conn_ctl, ids, 'UNKNOWN', None, None, str(e))
        raise

    except (SilverPreWriteError, SilverRollbackError) as e:
        _auditoria_cierre(conn_ctl, ids, 'ERROR', None, None, str(e))
        raise

    finally:
        if candado:
            _liberar_candado(conn_ctl, fuente)
        _cerrar_conn(conn_ctl, f'control-phone-{fuente}')
