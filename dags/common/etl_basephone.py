# ═══════════════════════════════════════════════════════
# etl_basephone.py
# Objetivo: Motor TRUNCATE+INSERT reutilizable para phones
# Carpeta: common/
# Versión: 2.3 — 2026-06-22 (TRUNCATE → SQL externo + rollback seguro)
# ═══════════════════════════════════════════════════════
# CAMBIOS v2.0:
#   - execute row-by-row → executemany por lotes de 1000
#   - SQL embebido → archivos externos sql/phones/
#   - dag_id como parámetro — identidad en logs
#   - traceback.format_exc() — error exacto con línea culpable
#   - try/except/finally — conexiones siempre se cierran
#   - Código comentado viejo eliminado — ver etl_basephone.py.bk
# CAMBIOS v2.1:
#   - Transacción única — commit al final, rollback si falla
#   - tabla nunca queda a medias
# CAMBIOS v2.2:
#   - Log detallado ANTES del rollback:
#     lote donde falló, filas procesadas, traceback completo
#     confirmación de rollback con nombre de tabla
# CAMBIOS v2.3:
#   - TRUNCATE → SQL externo sql/phones/truncate_phone.sql
#   - Rollback seguro — verifica que conn_destino exista
#     antes de hacer rollback (evita UnboundLocalError si
#     falla antes de abrir conexión)
#   - Cero SQL embebido en Python
# ═══════════════════════════════════════════════════════
# PENDIENTE v3.0:
#   - Migrar a UPSERT cuando el origen tenga llave natural
#     por teléfono (hoy solo tiene clientid + PHONE)
# ═══════════════════════════════════════════════════════
import traceback
from datetime                                       import datetime
from airflow.providers.mysql.hooks.mysql            import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql  import MsSqlHook
from common.sql_loader                              import cargar_sql

BATCH_SIZE     = 1000
SQL_SELECT     = "sql/phones/select_phone.sql"
SQL_INSERT     = "sql/phones/insert_phone.sql"
SQL_TRUNCATE   = "sql/phones/truncate_phone.sql"


def ejecutar_truncate_insert(
    dag_id          : str
  , mariadb_conn_id : str
  , mssql_conn_id   : str
  , vista_origen    : str
  , tabla_destino   : str
) -> int:
    """
    Ejecuta TRUNCATE + INSERT en lotes de 1000 filas con executemany.
    Transacción única — commit al final, rollback seguro si falla.
    Log detallado antes del rollback para diagnóstico.
    Cero SQL embebido en Python.

    Args:
        dag_id          : Identificador del DAG para logs
        mariadb_conn_id : ID conexión Airflow → MariaDB origen
        mssql_conn_id   : ID conexión Airflow → SQL Server destino
        vista_origen    : Vista MariaDB origen (db_general.vwpersonalinfo bb/fb/ml/fi/vc)
        tabla_destino   : Tabla SQL Server destino (source.Phone bb/fb/ml/fi/vc)

    Returns:
        Total de filas insertadas
    """
    print(f"[DAG: {dag_id}] — Iniciando TRUNCATE+INSERT | destino: {tabla_destino}")

    # ── Cargar SQL externos ───────────────────────────────
    query_truncate = cargar_sql(SQL_TRUNCATE, tabla_destino=tabla_destino)
    query_select   = cargar_sql(SQL_SELECT,   vista_origen=vista_origen)
    query_insert   = cargar_sql(SQL_INSERT,   tabla_destino=tabla_destino)

    # ── Conexiones ────────────────────────────────────────
    hook_origen  = MySqlHook(mysql_conn_id=mariadb_conn_id)
    hook_destino = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn_origen  = None
    conn_destino = None

    try:
        conn_origen  = hook_origen.get_conn()
        conn_destino = hook_destino.get_conn()

        cursor_origen  = conn_origen.cursor()
        cursor_destino = conn_destino.cursor()
        etl_fecha      = datetime.now()

        # ── TRUNCATE destino — commit inmediato ───────────
        # No forma parte de la transacción principal
        # Es intencional: libera espacio antes de insertar
        cursor_destino.execute(query_truncate)
        conn_destino.commit()
        print(f"[DAG: {dag_id}] — TRUNCATE {tabla_destino} OK")

        # ── SELECT origen ─────────────────────────────────
        cursor_origen.execute(query_select)
        filas_insertadas = 0

        # ── INSERT en lotes de 1000 (executemany) ─────────
        # SIN commit por lote — transacción única hasta el final
        while True:
            filas = cursor_origen.fetchmany(BATCH_SIZE)
            if not filas:
                break

            # Agregar atInsert — atUpdate siempre NULL en TRUNCATE+INSERT
            lote = [fila + (etl_fecha,) for fila in filas]

            # executemany → un solo roundtrip por lote de 1000
            cursor_destino.executemany(query_insert, lote)
            filas_insertadas += len(lote)

        # ── COMMIT único al final — todo o nada ───────────
        conn_destino.commit()
        print(f"[DAG: {dag_id}] — COMMIT OK | Filas insertadas: {filas_insertadas}")
        return filas_insertadas

    except Exception as e:
        # ── Log ANTES del rollback — diagnóstico completo ─
        print(f"[DAG: {dag_id}] — ERROR detectado en lote {filas_insertadas // BATCH_SIZE + 1}")
        print(f"[DAG: {dag_id}] — Filas procesadas antes del fallo: {filas_insertadas}")
        print(f"[DAG: {dag_id}] — {traceback.format_exc()}")
        # ── ROLLBACK seguro — solo si la conexión existe ──
        if conn_destino is not None:
            conn_destino.rollback()
            print(f"[DAG: {dag_id}] — ROLLBACK ejecutado — {tabla_destino} queda intacta")
        raise  # ← re-lanza para que Airflow marque FAILED

    finally:
        if conn_origen  is not None: conn_origen.close()
        if conn_destino is not None: conn_destino.close()
        print(f"[DAG: {dag_id}] — Conexiones cerradas")
