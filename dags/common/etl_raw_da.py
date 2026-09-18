# ═══════════════════════════════════════════════════════
# etl_raw_da.py
# Capa    : Datos (DA)
# Objetivo: Proveer conexiones, procedimientos y ejecución
#           de SPs CDC para las tablas RAW de clientes
# Carpeta : common/
# Versión : 1.3 — 2026-09-18
#   v1.0: versión inicial
#   v1.1: INSERT audit_log movido a archivo SQL externo
#   v1.2: ruta SQL corregida → sql/clients/insert_audit_log_raw.sql
#   v1.3: audit_tabla 242 corregida → db_general.etl_audit_log
# ═══════════════════════════════════════════════════════
import mysql.connector
from datetime              import datetime
from airflow.hooks.base    import BaseHook
from common.db_connections import (
    ORIGEN_CONN_ID_242
  , ORIGEN_CONN_ID_240
  , LOG_PATH
)
from common.sql_loader     import cargar_sql

# ── Ruta SQL externa ─────────────────────────────────────
SQL_INSERT_AUDIT = 'sql/clients/insert_audit_log_raw.sql'

# ── Mapa de instancias ───────────────────────────────────
_INSTANCIAS = {
    '242': {
        'conn_id'        : ORIGEN_CONN_ID_242
      , 'audit_conn'     : ORIGEN_CONN_ID_242
      , 'audit_tabla'    : 'db_general.etl_audit_log'  # ← corregido v1.3
      , 'procedimientos' : [
            'db_general.etl_clientsfb'
          , 'db_general.etl_clientsbb'
          , 'db_general.etl_clientsml'
        ]
    },
    '240': {
        'conn_id'        : ORIGEN_CONN_ID_240
      , 'audit_conn'     : ORIGEN_CONN_ID_240
      , 'audit_tabla'    : 'db_general.etl_audit_log'
      , 'procedimientos' : [
            'db_general.etl_clientsfi'
          , 'db_general.etl_clientsvc'
        ]
    },
}


# ════════════════════════════════════════════════════════
# Funciones de consulta — devuelven configuración
# ════════════════════════════════════════════════════════

def get_conexion(instancia: str) -> str:
    """Devuelve el conn_id de Airflow para la instancia."""
    return _INSTANCIAS[instancia]['conn_id']


def get_procedimientos(instancia: str) -> list:
    """Devuelve la lista de SPs CDC de la instancia."""
    return _INSTANCIAS[instancia]['procedimientos']


def get_log_config(instancia: str) -> dict:
    """
    Devuelve la configuración de auditoría para la instancia.
    Returns:
        {
          'conn_id': str   ← conexión Airflow al log
          'tabla'  : str   ← tabla de auditoría
          'path'   : str   ← ruta para log .txt
        }
    """
    return {
        'conn_id': _INSTANCIAS[instancia]['audit_conn']
      , 'tabla'  : _INSTANCIAS[instancia]['audit_tabla']
      , 'path'   : LOG_PATH
    }


# ════════════════════════════════════════════════════════
# Función de conexión — uso interno
# ════════════════════════════════════════════════════════

def _get_conn(conn_id: str):
    """
    Crea conexión mysql-connector-python
    usando credenciales de Airflow.
    Patrón idéntico a db_datasync._get_conn()
    """
    c = BaseHook.get_connection(conn_id)
    return mysql.connector.connect(
        host       = c.host
      , port       = c.port or 3306
      , user       = c.login
      , password   = c.password
      , database   = c.schema
      , autocommit = True   # ← SPs manejan su propio commit
      , use_pure   = True
    )


# ════════════════════════════════════════════════════════
# Ejecución de SP — NO maneja errores, lanza excepción
# ════════════════════════════════════════════════════════

def ejecutar_sp(conn_id: str, sp: str) -> None:
    """
    Ejecuta un stored procedure CDC en MariaDB.
    NO maneja errores — lanza excepción si falla.
    El manejo de errores es responsabilidad de la capa negocio.

    Args:
        conn_id : conn_id de Airflow para la instancia
        sp      : nombre completo del SP (ej. 'db_general.etl_clientsfb')
    """
    conn   = None
    nombre = sp.split('.')[-1]
    try:
        conn   = _get_conn(conn_id)
        cursor = conn.cursor()
        print(f"[{datetime.now()}] Ejecutando CALL {sp}()...")
        cursor.execute(f"CALL {sp}()")
        cursor.close()
        print(f"[{datetime.now()}] {nombre} — OK ✅")
    finally:
        if conn:
            conn.close()


# ════════════════════════════════════════════════════════
# Registro de auditoría en BD — SQL externo
# ════════════════════════════════════════════════════════

def registrar_log(
    instancia     : str
  , paquete       : str
  , sp            : str
  , estado        : str
  , mensaje_error : str      = None
  , fecha_inicio  : datetime = None
) -> None:
    """
    Inserta un registro en etl_audit_log de la instancia.
    SQL cargado de sql/clients/insert_audit_log_raw.sql
    NO maneja errores — lanza excepción si falla.

    Args:
        instancia     : '242' o '240'
        paquete       : nombre del DAG
        sp            : nombre completo del SP ejecutado
        estado        : 'OK' o 'ERROR'
        mensaje_error : texto del error (None si OK)
        fecha_inicio  : datetime de inicio del SP
    """
    cfg    = get_log_config(instancia)
    conn   = None
    ahora  = datetime.now()
    inicio = fecha_inicio or ahora

    sql = cargar_sql(SQL_INSERT_AUDIT, audit_tabla=cfg['tabla'])

    valores = (
        paquete
      , sp
      , f"db_general.tbl_raw_clients{sp[-2:]}"
      , 'DIARIO-CDC'
      , estado
      , mensaje_error[:500] if mensaje_error else None
      , inicio.strftime('%Y-%m-%d %H:%M:%S')
      , ahora.strftime('%Y-%m-%d %H:%M:%S')
    )

    try:
        conn   = _get_conn(cfg['conn_id'])
        cursor = conn.cursor()
        cursor.execute(sql, valores)
        cursor.close()
    finally:
        if conn:
            conn.close()
