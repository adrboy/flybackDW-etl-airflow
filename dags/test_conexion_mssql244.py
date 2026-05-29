from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import os

def test_mssql_hook():
    log_path = "/opt/airflow/logs"
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    log_file = os.path.join(log_path, f"testMSSQL_hook_{timestamp}.log")

    try:
        hook = MsSqlHook(mssql_conn_id='MSSQL244')
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT GETDATE()")
        row = cursor.fetchone()
        conn.close()

        with open(log_file, "w") as f:
            f.write(f"[{datetime.now()}] CONEXION EXITOSA via Hook\n")
            f.write(f"[{datetime.now()}] SQL Server fecha/hora: {row[0]}\n")
            f.write(f"[{datetime.now()}] Conn Id: MSSQL244\n")
            f.write(f"[{datetime.now()}] Base de datos: DBGeneralDW\n")

        print(f"CONEXION EXITOSA - Log guardado en: {log_file}")

    except Exception as e:
        with open(log_file, "w") as f:
            f.write(f"[{datetime.now()}] ERROR DE CONEXION\n")
            f.write(f"[{datetime.now()}] Detalle: {str(e)}\n")
        print(f"ERROR: {str(e)}")
        raise

with DAG(
    dag_id="test_conexion_mssql244",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["mssql", "test"]
) as dag:
    tarea = PythonOperator(
        task_id="test_conexion_hook",
        python_callable=test_mssql_hook
    )
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import pymssql
# import os


# def test_mssql_connection():
#     log_path = "/opt/airflow/logs"
#     timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
#     log_file = os.path.join(log_path, f"testMSSQL_{timestamp}.log")

#     try:
#         conn = pymssql.connect(
#             server="192.168.10.244",
#             user="andres",
#             password="Playa2023*",
#             database="DBGeneralDW",
#             port=1433
#         )
#         cursor = conn.cursor()
#         cursor.execute("SELECT GETDATE()")
#         row = cursor.fetchone()
#         conn.close()

#         with open(log_file, "w") as f:
#             f.write(f"[{datetime.now()}] CONEXION EXITOSA\n")
#             f.write(f"[{datetime.now()}] SQL Server fecha/hora: {row[0]}\n")
#             f.write(f"[{datetime.now()}] Host: 192.168.10.244\n")
#             f.write(f"[{datetime.now()}] Base de datos: DBGeneralDW\n")

#         print(f"CONEXION EXITOSA - Log guardado en: {log_file}")

#     except Exception as e:
#         with open(log_file, "w") as f:
#             f.write(f"[{datetime.now()}] ERROR DE CONEXION\n")
#             f.write(f"[{datetime.now()}] Detalle: {str(e)}\n")
#         print(f"ERROR: {str(e)}")
#         raise


# with DAG(
#     dag_id="test_conexion_mssql244",
#     start_date=datetime(2026, 1, 1),
#     schedule_interval=None,
#     catchup=False,
#     tags=["mssql", "test"]
# ) as dag:

#     tarea = PythonOperator(
#         task_id="test_conexion",
#         python_callable=test_mssql_connection
#     )
