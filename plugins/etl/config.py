# plugins/etl/config.py
from pathlib import Path

LAKE      = Path("/opt/airflow/data_lake")
CONN_STR  = "mysql://usuario:password@host.docker.internal/flybackDW"
DB_CONFIG = {
    "host"    : "host.docker.internal",
    "user"    : "usuario",
    "password": "password",
    "database": "flybackDW",
    "charset" : "utf8mb4",
}