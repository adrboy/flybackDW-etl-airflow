"""
DAG: test_conexion_mariadb
Objetivo: Verificar que Airflow puede conectarse a MariaDB flybackDW
"""

from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime

with DAG(
    dag_id            = "test_conexion_mariadb",
    description       = "Prueba de conexión a MariaDB flybackDW",
    schedule_interval = None,   # Solo manual
    start_date        = datetime(2026, 1, 1),
    catchup           = False,
    tags              = ["test", "mariadb"],
) as dag:

    test_conexion = MySqlOperator(
        task_id       = "test_conexion",
        mysql_conn_id = "MariaDB",
        sql           = "SELECT 'Conexion exitosa a flybackDW' AS mensaje;",
    )
