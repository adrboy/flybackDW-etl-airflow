"""
DAG: test_conexion_mariadb
Objetivo: Verificar que Airflow puede conectarse a las instancias MariaDB
Versión: 2.0 — 2026-08-25 (actualizado para Airflow 2.11.x)
"""

from airflow                                      import DAG
from airflow.providers.common.sql.operators.sql   import SQLExecuteQueryOperator
from datetime                                     import datetime

with DAG(
    dag_id            = "test_conexion_mariadb"
  , description       = "Prueba de conexión a instancias MariaDB — flybackDW, 240, 242"
  , schedule_interval = None   # Solo manual
  , start_date        = datetime(2026, 1, 1)
  , catchup           = False
  , tags              = ["test", "mariadb"]
) as dag:

    # ── MariaDB principal (flybackDW) ─────────────────────
    test_mariadb = SQLExecuteQueryOperator(
        task_id    = "test_mariadb_flybackDW"
      , conn_id    = "MariaDB"
      , sql        = "SELECT 'Conexion exitosa a flybackDW' AS mensaje;"
    )

    # ── MariaDB 240 ───────────────────────────────────────
    test_mariadb_240 = SQLExecuteQueryOperator(
        task_id    = "test_mariadb_240"
      , conn_id    = "MariaDB240"
      , sql        = "SELECT 'Conexion exitosa a MariaDB 240' AS mensaje;"
    )

    # ── MariaDB 242 ───────────────────────────────────────
    test_mariadb_242 = SQLExecuteQueryOperator(
        task_id    = "test_mariadb_242"
      , conn_id    = "MariaDB242"
      , sql        = "SELECT 'Conexion exitosa a MariaDB 242' AS mensaje;"
    )

    # ── Ejecutar en paralelo ──────────────────────────────
    [test_mariadb, test_mariadb_240, test_mariadb_242]
