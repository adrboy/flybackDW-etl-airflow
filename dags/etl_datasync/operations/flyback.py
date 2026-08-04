# ═══════════════════════════════════════════════════════
# operations/flyback.py
# Objetivo : Sincronizar customers.fb_clients → db_general.flyback
# Origen   : 192.168.10.242  (customers) — MariaDB_flyback
# Destino  : 192.168.10.242  (db_general) — MariaDB (CONN_GLOBAL)
# Versión  : 4.0 — 2026-08-03
# Cambios  : Conexión dedicada MariaDB_flyback
# ═══════════════════════════════════════════════════════
from common.db_connections import CONN_FLYBACK, CONN_GLOBAL
from common.db_datasync    import sincronizar_entre_servidores

SQL_TRUNCATE = 'sql/datasync/truncate_flyback.sql'
SQL_SELECT   = 'sql/datasync/select_flyback.sql'
SQL_INSERT   = 'sql/datasync/insert_flyback.sql'
SQL_LOG      = "INSERT INTO db_general.log (description) VALUES ('insert flyback');"


def sincronizar_flyback(dag_id: str) -> int:
    return sincronizar_entre_servidores(
        dag_id          = dag_id,
        conn_id_origen  = CONN_FLYBACK,
        conn_id_destino = CONN_GLOBAL,
        sql_truncate    = SQL_TRUNCATE,
        sql_select      = SQL_SELECT,
        sql_insert      = SQL_INSERT,
        sql_log         = SQL_LOG,
    )
