# PowerShell & Anaconda — Referencia de Comandos
**Fecha:** 2026-06-24  
**Proyecto:** flybackDW-etl-airflow  
**Contexto:** Comandos ejecutados durante la jornada de investigación huérfanos, paridad de drivers ODBC y ajuste de schedules.

---

## 1. Diagnóstico de paquetes Python (Anaconda)

```powershell
# Verificar paquetes instalados en Anaconda
& "C:\Users\GUSA CAPITAL\anaconda3\python.exe" -m pip show pymysql sqlalchemy pandas python-dotenv pyodbc
```
> Alternativa a `pip show` cuando el path tiene espacios — usa `python -m pip`.

---

## 2. Instalar paquetes faltantes (Anaconda)

```powershell
# Instalar pymysql y python-dotenv
& "C:\Users\GUSA CAPITAL\anaconda3\python.exe" -m pip install pymysql python-dotenv
```

---

## 3. Verificar drivers ODBC disponibles en Anaconda

```powershell
# Lista de drivers ODBC que ve el Python de Anaconda
& "C:\Users\GUSA CAPITAL\anaconda3\python.exe" -c "import pyodbc; print(pyodbc.drivers())"
```

---

## 4. Verificar drivers ODBC instalados en el sistema (Registro de Windows)

```powershell
# Consulta directa al registro de Windows — más completo que pyodbc.drivers()
Get-ItemProperty "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" | Select-Object *SQL*
```

---

## 5. Verificar drivers ODBC disponibles en Docker (Airflow)

```powershell
# Lista de drivers ODBC dentro del contenedor airflow_scheduler
docker exec -it airflow_scheduler python -c "import pyodbc; print(pyodbc.drivers())"
```

---

## 6. Listar contenedores Docker activos

```powershell
# Ver nombres y estado de contenedores — útil para identificar el nombre exacto
docker ps --format "table {{.Names}}\t{{.Status}}"
```

---

## 7. Ejecutar script utilitario MariaDB → SQL Server

```powershell
# Copia una tabla de MariaDB 242 hacia SQL Server 244
# Configurable: editar SQL_QUERY, DEST_TABLE y DEST_SCHEMA en el script
& "C:\Users\GUSA CAPITAL\anaconda3\python.exe" scripts/db_utils/mariadb_to_mssql.py
```
> Script en: `scripts/db_utils/mariadb_to_mssql.py`

---

## 8. Ejecutar script de inserción de huérfanos

```powershell
# Inserta los 5 clientids huérfanos en source.clientsfb
# Usa mismo SELECT/INSERT que el ETL de producción dag_clientsfb_242
& "C:\Users\GUSA CAPITAL\anaconda3\python.exe" scripts/db_utils/insert_huerfanos_clientsfb.py
```
> Script en: `scripts/db_utils/insert_huerfanos_clientsfb.py`

---

## 9. Reiniciar el scheduler de Airflow

```powershell
# Recargar DAGs después de cambios — siempre con comillas por el espacio en el path
cd "C:\Users\GUSA CAPITAL\Documents\DockersETL" && docker-compose restart airflow-scheduler
```

---

## Notas

| Tema | Detalle |
|---|---|
| Driver ODBC en Anaconda | Solo veía Driver 17 — se instaló Driver 18 desde Microsoft Learn |
| Driver ODBC en Docker | Solo tiene Driver 18 — paridad lograda |
| Descarga Driver 18 | https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server |
| Schedule dag_masterclients | Cambiado de `0 6 * * 1` (todos los lunes) a `0 6 * * 1#1` (primer lunes del mes) |
| Scripts utilitarios | `scripts/db_utils/` — laboratorio previo a producción, no afecta DAGs |
