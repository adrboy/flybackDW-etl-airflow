# ═══════════════════════════════════════════════════════
# operations/buyback.py
# Objetivo : Sincronizar buyback.clients → db_general.buyback
# Origen   : 192.168.10.242  (buyback) — MariaDB_buyback
# Destino  : 192.168.10.242  (db_general) — MariaDB (CONN_GLOBAL)
# Versión  : 4.0 — 2026-08-03
# Cambios  : Conexión dedicada MariaDB_buyback
# ═══════════════════════════════════════════════════════
from common.db_connections import CONN_BUYBACK, CONN_GLOBAL
from common.db_datasync    import sincronizar_entre_servidores

SQL_TRUNCATE = 'sql/datasync/truncate_buyback.sql'
SQL_SELECT   = 'sql/datasync/select_buyback.sql'
SQL_INSERT   = 'sql/datasync/insert_buyback.sql'
SQL_LOG      = "INSERT INTO db_general.log (description) VALUES ('insert buyback');"


def sincronizar_buyback(dag_id: str) -> int:
    return sincronizar_entre_servidores(
        dag_id          = dag_id,
        conn_id_origen  = CONN_BUYBACK,
        conn_id_destino = CONN_GLOBAL,
        sql_truncate    = SQL_TRUNCATE,
        sql_select      = SQL_SELECT,
        sql_insert      = SQL_INSERT,
        sql_log         = SQL_LOG,
    )
