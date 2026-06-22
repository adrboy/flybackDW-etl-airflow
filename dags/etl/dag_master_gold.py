# ═══════════════════════════════════════════════════════
# DAG: dag_master_gold
# Objetivo: Ejecutar SPs Gold Layer en SQL Server 244
# Carpeta: etl/
# Versión: 2.2 — 2026-06-22 (template_searchpath corregido)
# ═══════════════════════════════════════════════════════

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime                                   import datetime
import os

# ── Configuración ────────────────────────────────────────
# template_searchpath apunta a dags/ para que Jinja2
# encuentre los archivos .sql en sql/clients/
DAGS_DIR       = "/opt/airflow/dags"
SQL_SP_MAESTRO = "sql/clients/exec_sp_maestro.sql"
SQL_SP_PHONES  = "sql/clients/exec_sp_phones.sql"

# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id                = "dag_master_gold"
  , start_date            = datetime(2026, 1, 1)
  , schedule_interval     = "0 7 * * 1"
  , catchup               = False
  , tags                  = ["gold", "master"]
  , template_searchpath   = DAGS_DIR
) as dag:

    # SP 1 — Maestro ETL Gold
    sp_maestro = SQLExecuteQueryOperator(
        task_id       = "sp_etl_maestro"
      , conn_id       = "MSSQL244"
      , sql           = SQL_SP_MAESTRO
      , autocommit    = True
    )

    # SP 2 — Phones → factPersonalInfo
    sp_phones = SQLExecuteQueryOperator(
        task_id       = "sp_insert_phones"
      , conn_id       = "MSSQL244"
      , sql           = SQL_SP_PHONES
      , autocommit    = True
    )

    # Secuencia: maestro primero → phones después
    sp_maestro >> sp_phones
