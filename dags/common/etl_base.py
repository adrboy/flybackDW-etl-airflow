#import pymysql
#import pymssql
from datetime import datetime
from airflow.hooks.mysql_hook import MySqlHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# 242
#SELECT MAX(clientid) from [DBGeneralDW].[source].[clientsfb]
#SELECT MAX(clientid) from [DBGeneralDW].[source].[clientsbb]
#SELECT ISNULL(MAX(clientid), 0) from [DBGeneralDW].[source].[clientsml]

# 240
#SELECT MAX(clientid) from [DBGeneralDW].[source].[clientsfi]
#SELECT MAX(clientid) from [DBGeneralDW].[source].[clientsvc]

def get_max_id(mssql_conn_id, tabla_destino):
    hook = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn = hook.get_conn()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT ISNULL(MAX(clientid), 0) FROM {tabla_destino}")
        resultado = cursor.fetchone()
        return resultado[0]
    finally:
        conn.close()

def ejecutar_insert(
    # Conexiones via Hook de Airflow
    mariadb_conn_id,
    mssql_conn_id,
    # ETL
    vista_origen, tabla_destino, max_id
):
    # Conexión a MariaDB via Hook
    hook_origen = MySqlHook(mysql_conn_id=mariadb_conn_id)
    conn_origen = hook_origen.get_conn()

    # Conexión a SQL Server via Hook
    hook_destino = MsSqlHook(mssql_conn_id=mssql_conn_id)
    conn_destino = hook_destino.get_conn()

    try:
        cursor_origen = conn_origen.cursor()
        cursor_destino = conn_destino.cursor()

        cursor_origen.execute(f"""
            SELECT productid, contractid, clientid, email, capdata,
                   FirstName, LastName, countrycode, country, Estate,
                   ciudad, address, zip, corpcode, corp,
                   ingreso, egreso, rank, EstatusN, EstatusL
            FROM {vista_origen} 
            WHERE clientid > {max_id}
        """)
        filas_insertadas = 0
        batch_size = 1000
        etl_fecha = datetime.now()

        while True:
            filas = cursor_origen.fetchmany(batch_size)
            if not filas:
                break
            for fila in filas:
                sql_insert = f"""
                    INSERT INTO {tabla_destino} (
                        productid, contractid, clientid, email, capdata,
                        FirstName, LastName, countrycode, country, Estate,
                        ciudad, address, zip, corpcode, corp,
                        ingreso, egreso, rank, EstatusN, EstatusL,
                        createdAt, updatedAt, deletedAt
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, NULL, NULL
                    )
                """
                cursor_destino.execute(sql_insert, fila + (etl_fecha,))
            conn_destino.commit()
            filas_insertadas += len(filas)
        return filas_insertadas
    finally:
        conn_origen.close()
        conn_destino.close()
# def get_max_id(servidor, usuario, password, base_datos, tabla_destino):
#     conn = pymssql.connect(
#         server=servidor,
#         user=usuario,
#         password=password,
#         database=base_datos,
#         port=1433
#     )
#     try:
#         cursor = conn.cursor()
#         cursor.execute(f"SELECT ISNULL(MAX(clientid), 0) FROM {tabla_destino}")
#         resultado = cursor.fetchone()
#         return resultado[0]
#     finally:
#         conn.close()

# def ejecutar_insert(
#     # Conexión MariaDB desde Airflow
#     mariadb_conn_id,
#     # Destino SQL Server
#     destino_servidor, destino_usuario, destino_password, destino_base,
#     # ETL
#     vista_origen, tabla_destino, max_id
# ):
#     # Conexión a MariaDB via Hook de Airflow
#     hook = MySqlHook(mysql_conn_id=mariadb_conn_id)
#     conn_origen = hook.get_conn()

#     # Conexión a SQL Server
#     conn_destino = pymssql.connect(
#         server=destino_servidor,
#         user=destino_usuario,
#         password=destino_password,
#         database=destino_base,
#         port=1433
#     )
#     try:
#         cursor_origen = conn_origen.cursor()
#         cursor_destino = conn_destino.cursor()

#         # Ejecutar consulta en MariaDB
#         #cursor_origen.execute(f"SELECT * FROM {vista_origen} WHERE clientid > {max_id}")
#         cursor_origen.execute(f"""
#             SELECT productid, contractid, clientid, email, capdata,
#                    FirstName, LastName, countrycode, country, Estate,
#                    ciudad, address, zip, corpcode, corp,
#                    ingreso, egreso, rank, EstatusN, EstatusL
#             FROM {vista_origen} 
#             WHERE clientid > {max_id}
#         """)
#         filas_insertadas = 0
#         batch_size = 1000
#         etl_fecha = datetime.now()

#         while True:
#             filas = cursor_origen.fetchmany(batch_size)
#             if not filas:
#                 break
#             for fila in filas:
#                 placeholders = ", ".join(["%s"] * len(fila))                
#                 #sql_insert = f"INSERT INTO {tabla_destino} VALUES ({placeholders})"
#                 sql_insert = f"""
#                     INSERT INTO {tabla_destino} (
#                         productid, contractid, clientid, email, capdata,
#                         FirstName, LastName, countrycode, country, Estate,
#                         ciudad, address, zip, corpcode, corp,
#                         ingreso, egreso, rank, EstatusN, EstatusL,
#                         createdAt, updatedAt, deletedAt
#                     ) VALUES (
#                         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
#                         %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
#                         %s, NULL, NULL
#                     )
#                 """
#                 #cursor_destino.execute(sql_insert, fila)
#                 cursor_destino.execute(sql_insert, fila + (etl_fecha,))
#             conn_destino.commit()
#             filas_insertadas += len(filas)
#         return filas_insertadas
#     finally:
#         conn_origen.close()
#         conn_destino.close()


# def get_max_id(servidor, usuario, password, base_datos, tabla_destino):
#     conn = pymssql.connect(
#         server=servidor,
#         user=usuario,
#         password=password,
#         database=base_datos,
#         port=1433
#     )
#     try:
#         cursor = conn.cursor()
#         cursor.execute(f"SELECT ISNULL(MAX(clientid), 0) FROM {tabla_destino}")
#         resultado = cursor.fetchone()
#         return resultado[0]
#     finally:
#         conn.close()

# def ejecutar_insert(
#     # Origen MariaDB
#     origen_host, origen_usuario, origen_password, origen_base,
#     # Destino SQL Server
#     destino_servidor, destino_usuario, destino_password, destino_base,
#     # ETL
#     vista_origen, tabla_destino, max_id
# ):
#     # Conexión a MariaDB
#     conn_origen = pymysql.connect(
#         host=origen_host,
#         user=origen_usuario,
#         password=origen_password,
#         database=origen_base,
#         charset='utf8mb4',
#         port=3306
#     )
#     # Conexión a SQL Server
#     conn_destino = pymssql.connect(
#         server=destino_servidor,
#         user=destino_usuario,
#         password=destino_password,
#         database=destino_base,
#         port=1433
#     )
#     try:
#         cursor_origen = conn_origen.cursor()
#         cursor_destino = conn_destino.cursor()

#         # Ejecutar consulta en MariaDB
#         cursor_origen.execute(f"SELECT * FROM {vista_origen} WHERE clientid > {max_id}")
#         filas_insertadas = 0
#         batch_size = 1000

#         while True:
#             filas = cursor_origen.fetchmany(batch_size)
#             if not filas:
#                 break
#             for fila in filas:
#                 placeholders = ", ".join(["%s"] * len(fila))
#                 sql_insert = f"INSERT INTO {tabla_destino} VALUES ({placeholders})"
#                 cursor_destino.execute(sql_insert, fila)
#             conn_destino.commit()
#             filas_insertadas += len(filas)
#         return filas_insertadas 
#     finally:
#         conn_origen.close()
#         conn_destino.close()