# ═══════════════════════════════════════════════════════
# DAG: dag_master_gold
# Objetivo: Ejecutar SPs Gold Layer en SQL Server 244
# Carpeta: etl/
# Versión: 3.0 — 2026-06-23 (ExternalTaskSensor activo)
# ═══════════════════════════════════════════════════════
# CAMBIOS v3.0:
#   - ExternalTaskSensor activado para dag_masterclients
#     y dag_masterphones — Gold no corre hasta que ambos
#     Bronze terminen exitosamente
#   - Flujo: [esperar_clients, esperar_phones] >> sp_maestro >> sp_phones
# ═══════════════════════════════════════════════════════

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task              import ExternalTaskSensor
from datetime                                   import datetime

# ── Configuración ────────────────────────────────────────
DAGS_DIR       = "/opt/airflow/dags"
SQL_SP_MAESTRO = "sql/clients/exec_sp_maestro.sql"
SQL_SP_PHONES  = "sql/clients/exec_sp_phones.sql"

# ── DAG ───────────────────────────────────────────────────
with DAG(
    dag_id              = "dag_master_gold"
  , start_date          = datetime(2026, 6, 26)  # ← actualizado para evitar catchup
  , schedule_interval   = "0 7 * * 1#1"  # ← primer lunes del mes — sincronizado con Bronze
  , catchup             = False
  , tags                = ["gold", "master"]
  , template_searchpath = DAGS_DIR
) as dag:

    # ── Sensor 1 — Esperar Bronze Clients ────────────────
    # Gold no arranca hasta que dag_masterclients termine OK
    esperar_clients = ExternalTaskSensor(
        task_id          = "esperar_clients"
      , external_dag_id  = "dag_masterclients"
      , external_task_id = None        # ← espera el DAG completo
      , mode             = "reschedule"
      , timeout          = 7200        # ← 2 horas máximo
      , poke_interval    = 60          # ← verifica cada 60 seg
    )

    # ── Sensor 2 — Esperar Bronze Phones ─────────────────
    # Gold no arranca hasta que dag_masterphones termine OK
    esperar_phones = ExternalTaskSensor(
        task_id          = "esperar_phones"
      , external_dag_id  = "dag_masterphones"
      , external_task_id = None        # ← espera el DAG completo
      , mode             = "reschedule"
      , timeout          = 7200        # ← 2 horas máximo
      , poke_interval    = 60          # ← verifica cada 60 seg
    )

    # ── SP 1 — Maestro ETL Gold ───────────────────────────
    sp_maestro = SQLExecuteQueryOperator(
        task_id       = "sp_etl_maestro"
      , conn_id       = "MSSQL244"
      , sql           = SQL_SP_MAESTRO
      , autocommit    = True
    )

    # ── SP 2 — Phones → factPersonalInfo ─────────────────
    sp_phones = SQLExecuteQueryOperator(
        task_id       = "sp_insert_phones"
      , conn_id       = "MSSQL244"
      , sql           = SQL_SP_PHONES
      , autocommit    = True
    )

    # ── Secuencia ─────────────────────────────────────────
    # Ambos Bronze deben terminar antes de ejecutar Gold
    [esperar_clients, esperar_phones] >> sp_maestro >> sp_phones
