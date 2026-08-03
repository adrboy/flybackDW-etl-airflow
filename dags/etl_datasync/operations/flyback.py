# ═══════════════════════════════════════════════════════
# operations/flyback.py
# Objetivo : Sincronizar customers.fb_clients → db_general.flyback
# Origen   : 192.168.10.242  (customers)
# Destino  : 192.168.10.242  (db_general)
# Versión  : 3.5 — 2026-07-31
# Nota     : Mismo servidor — sincronizar() con una conexión
#            y dos cursores (cur_select + cur_insert)
# ═══════════════════════════════════════════════════════
from common.db_connections import ORIGEN_CONN_ID_242
from common.db_datasync    import sincronizar

# ── Configuración ────────────────────────────────────────
SQL_TRUNCATE = 'sql/datasync/truncate_flyback.sql'
SQL_SELECT   = 'sql/datasync/select_flyback.sql'
SQL_INSERT   = 'sql/datasync/insert_flyback.sql'
SQL_LOG      = "INSERT INTO db_general.log (description) VALUES ('insert flyback');"


# ── Función ETL ──────────────────────────────────────────
def sincronizar_flyback(dag_id: str) -> int:
    return sincronizar(
        dag_id       = dag_id,
        conn_id      = ORIGEN_CONN_ID_242,
        sql_truncate = SQL_TRUNCATE,
        sql_select   = SQL_SELECT,
        sql_insert   = SQL_INSERT,
        sql_log      = SQL_LOG,
    )
