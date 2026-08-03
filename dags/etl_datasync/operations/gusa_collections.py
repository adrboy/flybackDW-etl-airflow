# ═══════════════════════════════════════════════════════
# operations/gusa_collections.py
# Objetivo : Sincronizar financiamiento → db_general.gusa_collections
# Origen   : 192.168.10.240  (financiamiento)
# Destino  : 192.168.10.242  (db_general)
# Versión  : 3.3 — 2026-07-31
# Cambios  : Sin indices_fecha — sign fluye natural desde BD
# ═══════════════════════════════════════════════════════
from common.db_connections import ORIGEN_CONN_ID_240, ORIGEN_CONN_ID_242
from common.db_datasync    import sincronizar_entre_servidores

# ── Configuración ────────────────────────────────────────
SQL_TRUNCATE = 'sql/datasync/truncate_gusa_collections.sql'
SQL_SELECT   = 'sql/datasync/select_gusa_collections.sql'
SQL_INSERT   = 'sql/datasync/insert_gusa_collections.sql'
SQL_LOG      = "INSERT INTO db_general.log (description) VALUES ('insert gusa_collections');"


# ── Función ETL ──────────────────────────────────────────
def sincronizar_gusa_collections(dag_id: str) -> int:
    return sincronizar_entre_servidores(
        dag_id          = dag_id,
        conn_id_origen  = ORIGEN_CONN_ID_240,
        conn_id_destino = ORIGEN_CONN_ID_242,
        sql_truncate    = SQL_TRUNCATE,
        sql_select      = SQL_SELECT,
        sql_insert      = SQL_INSERT,
        sql_log         = SQL_LOG,
    )
