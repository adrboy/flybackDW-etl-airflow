# ═══════════════════════════════════════════════════════
# dag_masterclients.py
# DESACTIVADO — 2026-09-17
# Motivo  : Reemplazado por dag_raw_clients_cdc_diario
#           que usa SPs CDC directos en MariaDB RAW
# Pendiente: Reutilizar para Silver quincenal/mensual
#            RAW → SQL Server homologado
# ═══════════════════════════════════════════════════════
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id            = "dag_masterclients"
  , start_date        = datetime(2026, 6, 26)
  , schedule_interval = None   # ← DESACTIVADO — era "0 6 * * 1#1"
  , catchup           = False
  , tags              = ["bronze", "master", "desactivado"]
) as dag:

    trigger_clientsfi = TriggerDagRunOperator(
        task_id        = "trigger_clientsfi"
      , trigger_dag_id = "dag_clientsfi_240"
      , wait_for_completion = True
    )
    trigger_clientsvc = TriggerDagRunOperator(
        task_id        = "trigger_clientsvc"
      , trigger_dag_id = "dag_clientsvc_240"
      , wait_for_completion = True
    )
    trigger_clientsfb = TriggerDagRunOperator(
        task_id        = "trigger_clientsfb"
      , trigger_dag_id = "dag_clientsfb_242"
      , wait_for_completion = True
    )
    trigger_clientsbb = TriggerDagRunOperator(
        task_id        = "trigger_clientsbb"
      , trigger_dag_id = "dag_clientsbb_242"
      , wait_for_completion = True
    )
    trigger_clientsml = TriggerDagRunOperator(
        task_id        = "trigger_clientsml"
      , trigger_dag_id = "dag_clientsml_242"
      , wait_for_completion = True
    )

    trigger_clientsfi >> trigger_clientsvc >> trigger_clientsfb >> trigger_clientsbb >> trigger_clientsml
