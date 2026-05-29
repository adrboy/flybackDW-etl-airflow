from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

sys.path.insert(0, '/opt/airflow/dags')
from common.etl_base import get_max_id, ejecutar_insert
from common.audit_logger import registrar_log, escribir_log_txt
from common.db_connections import (
    ORIGEN_CONN_ID_240,
    MSSQL_CONN_ID,
    LOG_PATH
)

VISTA_ORIGEN  = "db_general.viewclientsvc"
TABLA_DESTINO = "source.clientsvc"

def etl_clientsvc():
    fecha_inicio = datetime.now()
    mensaje_log  = []
    max_id = 0
    filas  = 0

    try:
        # Paso 1 - Obtener MAX clientid del destino
        max_id = get_max_id(
            MSSQL_CONN_ID, TABLA_DESTINO
        )
        mensaje_log.append(f"MAX clientid destino: {max_id}")

        # Paso 2 - Ejecutar insert
        filas = ejecutar_insert(
            ORIGEN_CONN_ID_240,
            MSSQL_CONN_ID,
            VISTA_ORIGEN, TABLA_DESTINO, max_id
        )
        mensaje_log.append(f"Filas insertadas: {filas}")
        estado = "SUCCESS"
        mensaje_error = None

    except Exception as e:
        mensaje_log.append(f"ERROR: {str(e)}")
        estado = "ERROR"
        mensaje_error = str(e)
        raise

    finally:
        # Log siempre se ejecuta — nunca reintenta
        try:
            registrar_log(
                paquete="etl_clientsvc_240",
                vista_origen=VISTA_ORIGEN,
                tabla_destino=TABLA_DESTINO,
                max_id_inicio=max_id,
                filas_insertadas=filas,
                tipo_ejecucion="SCHEDULED",
                estado=estado,
                mensaje_error=mensaje_error,
                fecha_inicio=fecha_inicio,
                fecha_fin=datetime.now()
            )
            escribir_log_txt(LOG_PATH, "clientsvc", "\n".join(mensaje_log))
        except Exception as log_error:
            print(f"WARNING: Log falló pero ETL fue exitoso: {str(log_error)}")

with DAG(
    dag_id="dag_clientsvc_240",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["bronze", "240", "clientsvc"]
) as dag:
    tarea_etl = PythonOperator(
        task_id="etl_clientsvc",
        python_callable=etl_clientsvc,
        retries=3,
        retry_delay=60
    )