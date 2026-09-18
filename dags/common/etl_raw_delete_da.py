# ═══════════════════════════════════════════════════════
# etl_raw_delete_da.py
# Capa    : Datos (DA)
# Objetivo: Proveer conexiones y procedimientos DELETE
#           para las tablas RAW de clientes
# Carpeta : common/
# Versión : 1.0 — 2026-09-18
# ═══════════════════════════════════════════════════════
import mysql.connector
from datetime              import datetime
from airflow.hooks.base    import BaseHook
from common.db_connections import (
    ORIGEN_CONN_ID_242
  , ORIGEN_CONN_ID_240
  , LOG_PATH
)

# ── Mapa de instancias ───────────────────────────────────
_INSTANCIAS = {
    '242': {
        'conn_id'        : ORIGEN_CONN_ID_242
      , 'audit_conn'     : ORIGEN_CONN_ID_242
      , 'audit_tabla'    : 'db_general.etl_audit_log'
      , 'procedimientos' : [
            'db_general.etl_clientsfb_delete'
          , 'db_general.etl_clientsbb_delete'
          , 'db_general.etl_clientsml_delete'
        ]
    },
    '240': {
        'conn_id'        : ORIGEN_CONN_ID_240
      , 'audit_conn'     : ORIGEN_CONN_ID_240
      , 'audit_tabla'    : 'db_general.etl_audit_log'
      , 'procedimientos' : [
            'db_general.etl_clientsfi_delete'
          , 'db_general.etl_clientsvc_delete'
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
    """Devuelve la lista de SPs DELETE de la instancia."""
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
    Patrón idéntico a etl_raw_da._get_conn()
    """
    c = BaseHook.get_connection(conn_id)
    return mysql.connector.connect(
        host       = c.host
      , port       = c.port or 3306
      , user       = c.login
      , password   = c.password
      , database   = c.schema
      , autocommit = True
      , use_pure   = True
    )


# ════════════════════════════════════════════════════════
# Ejecución de SP DELETE — NO maneja errores
# ════════════════════════════════════════════════════════

def ejecutar_sp(conn_id: str, sp: str) -> None:
    """
    Ejecuta un stored procedure DELETE en MariaDB.
    NO maneja errores — lanza excepción si falla.
    El manejo de errores es responsabilidad de la capa negocio.

    Args:
        conn_id : conn_id de Airflow para la instancia
        sp      : nombre completo del SP DELETE
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
