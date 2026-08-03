# ═══════════════════════════════════════════════════════
# operations/buyback.py
# Objetivo : Sincronizar buyback.clients → db_general.buyback
# Origen   : 192.168.10.242  (buyback)
# Destino  : 192.168.10.242  (db_general)
# Versión  : 3.3 — 2026-07-31
# Cambios  : Sin indices_fecha — fechas manejadas en SQL
#            sign  → NULL natural
#            activated → NULLIF en select_buyback.sql
# ═══════════════════════════════════════════════════════
from common.db_connections import ORIGEN_CONN_ID_242
from common.db_datasync    import sincronizar

# ── Configuración ────────────────────────────────────────
SQL_TRUNCATE = 'sql/datasync/truncate_buyback.sql'
SQL_SELECT   = 'sql/datasync/select_buyback.sql'
SQL_INSERT   = 'sql/datasync/insert_buyback.sql'
SQL_LOG      = "INSERT INTO db_general.log (description) VALUES ('insert buyback');"


# ── Función ETL ──────────────────────────────────────────
def sincronizar_buyback(dag_id: str) -> int:
    return sincronizar(
        dag_id       = dag_id,
        conn_id      = ORIGEN_CONN_ID_242,
        sql_truncate = SQL_TRUNCATE,
        sql_select   = SQL_SELECT,
        sql_insert   = SQL_INSERT,
        sql_log      = SQL_LOG,
    )
