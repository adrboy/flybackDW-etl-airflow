# ═══════════════════════════════════════════════════════
# etl_raw_phone_da.py
# Capa    : Datos (DA)
# Objetivo: Proveer conexiones, procedimientos y ejecución
#           de los SPs RAW de teléfonos (insertar nuevos,
#           revivir y marcar deletedAt) con compuerta de seguridad
# Carpeta : common/
# Versión : 1.0 — BORRADOR pendiente de pruebas desde Airflow
# Nota    : Patrón de etl_raw_da.py, pero los SPs de teléfonos
#           reciben (p_max_pct, p_forzar) y devuelven una fila
#           (nuevos, revividos, marcados, vivos_final, segundos).
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

# ── Constantes ───────────────────────────────────────────
SQL_INSERT_AUDIT = 'sql/phones/insert_audit_log_raw_phone.sql'
TIPO_EJECUCION   = 'RAW-PHONE'      # varchar(20) en etl_audit_log
MAX_PCT_DEFAULT  = 2                # % máximo de filas vivas que el SP puede marcar como borradas

# Límite por SP si alguno necesita otro valor (clave = nombre completo del SP)
_MAX_PCT = {
    # 'db_general.sp_raw_phoneml' : 5
}

# ── Mapa de instancias ───────────────────────────────────
_INSTANCIAS = {
    '242': {
        'conn_id'        : ORIGEN_CONN_ID_242
      , 'audit_conn'     : ORIGEN_CONN_ID_242
      , 'audit_tabla'    : 'db_general.etl_audit_log'
      , 'procedimientos' : [
            # 'db_general.sp_raw_phonefb'    # fb: descomentar cuando se decida habilitarlo
            'db_general.sp_raw_phonebb'
          , 'db_general.sp_raw_phoneml'
        ]
    },
    '240': {
        'conn_id'        : ORIGEN_CONN_ID_240
      , 'audit_conn'     : ORIGEN_CONN_ID_240
      , 'audit_tabla'    : 'db_general.etl_audit_log'
      , 'procedimientos' : [
            'db_general.sp_raw_phonefi'
          , 'db_general.sp_raw_phonevc'
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
    """Devuelve la lista de SPs RAW de teléfonos de la instancia."""
    return _INSTANCIAS[instancia]['procedimientos']


def get_max_pct(sp: str) -> float:
    """Devuelve el límite de la compuerta para el SP (por defecto MAX_PCT_DEFAULT)."""
    return _MAX_PCT.get(sp, MAX_PCT_DEFAULT)


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
      , autocommit = True   # ← los SPs manejan su propio flujo
      , use_pure   = True
    )


# ════════════════════════════════════════════════════════
# Ejecución de SP — NO maneja errores, lanza excepción
# ════════════════════════════════════════════════════════

def ejecutar_sp(conn_id: str, sp: str, max_pct: float = MAX_PCT_DEFAULT, forzar: int = 0) -> dict:
    """
    Ejecuta un SP RAW de teléfonos y devuelve la fila de resultado.
    NO maneja errores — lanza excepción si falla. Si la compuerta
    bloquea, el SP lanza error 1644 con texto 'BLOQUEADO: ...'.

    Args:
        conn_id : conn_id de Airflow para la instancia
        sp      : nombre completo del SP (ej. 'db_general.sp_raw_phonebb')
        max_pct : % máximo de filas vivas que se permite marcar
        forzar  : 1 = ignorar la compuerta

    Returns:
        {'nuevos': n, 'revividos': n, 'marcados': n, 'vivos_final': n, 'segundos': n}
    """
    conn      = None
    nombre    = sp.split('.')[-1]
    resultado = {}
    try:
        conn   = _get_conn(conn_id)
        cursor = conn.cursor()
        print(f"[{datetime.now()}] Ejecutando CALL {sp}({max_pct}, {forzar})...")
        cursor.callproc(sp, (max_pct, forzar))
        for rs in cursor.stored_results():
            columnas = rs.column_names
            fila     = rs.fetchone()
            if fila:
                resultado = dict(zip(columnas, fila))
        cursor.close()
        print(f"[{datetime.now()}] {nombre} — OK ✅ {resultado}")
        return resultado
    finally:
        if conn:
            conn.close()


# ════════════════════════════════════════════════════════
# Registro de auditoría en BD — SQL externo
# ════════════════════════════════════════════════════════

def registrar_log(
    instancia        : str
  , paquete          : str
  , sp               : str
  , estado           : str
  , filas_insertadas : int      = None
  , mensaje_error    : str      = None
  , fecha_inicio     : datetime = None
) -> None:
    """
    Inserta un registro en etl_audit_log de la instancia.
    SQL cargado de sql/phones/insert_audit_log_raw_phone.sql
    NO maneja errores — lanza excepción si falla.

    Args:
        instancia        : '242' o '240'
        paquete          : nombre del DAG
        sp               : nombre completo del SP ejecutado
        estado           : 'OK', 'ERROR' o 'BLOQUEADO'
        filas_insertadas : teléfonos nuevos de la corrida (None si falló)
        mensaje_error    : texto del error (None si OK)
        fecha_inicio     : datetime de inicio del SP
    """
    cfg    = get_log_config(instancia)
    conn   = None
    ahora  = datetime.now()
    inicio = fecha_inicio or ahora
    sufijo = sp.split('.')[-1].replace('sp_raw_phone', '')   # fb / bb / ml / fi / vc

    sql = cargar_sql(SQL_INSERT_AUDIT, audit_tabla=cfg['tabla'])

    valores = (
        paquete
      , sp
      , f"db_general.tbl_raw_phone{sufijo}"
      , TIPO_EJECUCION
      , estado
      , filas_insertadas
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
