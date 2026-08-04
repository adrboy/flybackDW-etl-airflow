# ── Conexiones generales ─────────────────────────────────
ORIGEN_CONN_ID_242 = 'MariaDB'       # 192.168.10.242 — db_general
ORIGEN_CONN_ID_240 = 'MariaDB240'    # 192.168.10.240 — general

# ── Conexiones dedicadas por BD — etl_datasync ───────────
# Equivalente a las conexiones individuales del C# original:
# dbGusa, dbFlyBack, dbBuyBack, dbVC, dbGlobal
CONN_GUSA    = 'MariaDB_gusa'        # 192.168.10.240 — financiamiento
CONN_FLYBACK = 'MariaDB_flyback'     # 192.168.10.242 — customers
CONN_BUYBACK = 'MariaDB_buyback'     # 192.168.10.242 — buyback
CONN_VTW     = 'MariaDB_vtw'         # 192.168.10.240 — vtw
CONN_GLOBAL  = 'MariaDB_global'      # 192.168.10.242 — db_general (dedicada)

# ── SQL Server ───────────────────────────────────────────
MSSQL_CONN_ID = 'MSSQL244'

# ── ETL ──────────────────────────────────────────────────
LOG_PATH = "/opt/airflow/logs"
