import pymysql
import os
from datetime import datetime
from airflow.hooks.mysql_hook import MySqlHook

def registrar_log(
    paquete,
    vista_origen,
    tabla_destino,
    max_id_inicio,
    filas_insertadas,
    tipo_ejecucion,
    estado,
    mensaje_error,
    fecha_inicio,
    fecha_fin
):
    # Conexión a la base de datos
    # connection = pymysql.connect(
    #     host="192.168.10.242",
    #     user="andres",
    #     password="Playa2023*",
    #     database="flybackDW",
    #     port=3306
    # )
    
     # Conexión a MariaDB via Hook de Airflow
    hook = MySqlHook(mysql_conn_id='MariaDB')
    connection = hook.get_conn()

    try:
        with connection.cursor() as cursor:
            sql = """
                INSERT INTO flybackDW.etl_audit_log (
                    paquete,
                    vista_origen,
                    tabla_destino,
                    max_id_inicio,
                    filas_insertadas,
                    tipo_ejecucion,
                    estado,
                    mensaje_error,
                    fecha_inicio,
                    fecha_fin
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (
                paquete,
                vista_origen,
                tabla_destino,
                max_id_inicio,
                filas_insertadas,
                tipo_ejecucion,
                estado,
                mensaje_error,
                fecha_inicio.strftime("%Y-%m-%d %H:%M:%S"),
                fecha_fin.strftime("%Y-%m-%d %H:%M:%S")
            ))
            connection.commit()
    finally:
        connection.close()

def escribir_log_txt(log_path, vista, mensaje):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    nombre_archivo = f"etl_{vista}_FB_log_{timestamp}.txt"
    ruta_completa = os.path.join(log_path, nombre_archivo)

    with open(ruta_completa, "a") as f:
        f.write(f"[{datetime.now()}] {mensaje}\n")

    return ruta_completa