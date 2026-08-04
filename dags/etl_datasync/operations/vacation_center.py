# ═══════════════════════════════════════════════════════
# operations/vacation_center.py
# Objetivo : Sincronizar vtw.p_data → db_general.vtw
# Origen   : 192.168.10.240  (vtw) — MariaDB_vtw
# Destino  : 192.168.10.242  (db_general) — MariaDB (CONN_GLOBAL)
# Versión  : 4.0 — 2026-08-03
# Cambios  : Conexión dedicada MariaDB_vtw
# ═══════════════════════════════════════════════════════
from common.db_connections import CONN_VTW, CONN_GLOBAL
from common.db_datasync    import sincronizar_entre_servidores

SQL_TRUNCATE = 'sql/datasync/truncate_vacation_center.sql'
SQL_SELECT   = 'sql/datasync/select_vacation_center.sql'
SQL_INSERT   = 'sql/datasync/insert_vacation_center.sql'
SQL_LOG      = "INSERT INTO db_general.log (description) VALUES ('insert vtw');"


def sincronizar_vacation_center(dag_id: str) -> int:
    return sincronizar_entre_servidores(
        dag_id          = dag_id,
        conn_id_origen  = CONN_VTW,
        conn_id_destino = CONN_GLOBAL,
        sql_truncate    = SQL_TRUNCATE,
        sql_select      = SQL_SELECT,
        sql_insert      = SQL_INSERT,
        sql_log         = SQL_LOG,
    )
