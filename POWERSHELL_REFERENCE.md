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

## 10. Auditoría del motor de sincronización

```powershell
# Verificar sincronización de tblInicioSolicitados, Autorizados y Pagados
& "C:\Users\GUSA CAPITAL\anaconda3\python.exe" scripts/db_utils/audit_engine.py
```
> Script en: `scripts/db_utils/audit_engine.py` | Catálogo: `scripts/db_utils/sync_config.json`

---

## 11. Disparo manual de un DAG

```powershell
# Disparar cualquier DAG manualmente
docker exec -it airflow_scheduler airflow dags trigger <dag_id>
```

---

## 12. Ver estado de tareas de un run

```powershell
# Ver estado de cada tarea en un run específico
docker exec -it airflow_scheduler airflow tasks states-for-dag-run <dag_id> "<run_id>"
```

---

## 13. Limpieza completa de historial dag_run (PostgreSQL)

```powershell
# Conectarse al PostgreSQL de Airflow — puerto 5433
# Host: localhost | User: airflow | Pass: airflow | DB: airflow

# Auditar antes de borrar
docker exec -it airflow_postgres_dedicated psql -U airflow -c "SELECT dag_id, state, COUNT(*) FROM dag_run GROUP BY dag_id, state ORDER BY dag_id, state;"

# Pausar todos los DAGs antes de limpiar
docker exec -it airflow_postgres_dedicated psql -U airflow -c "UPDATE dag SET is_paused = true WHERE is_paused = false;"

# Borrar runs anteriores a hoy
docker exec -it airflow_postgres_dedicated psql -U airflow -c "DELETE FROM dag_run WHERE DATE(queued_at) < CURRENT_DATE;"

# Borrar absolutamente todo
docker exec -it airflow_postgres_dedicated psql -U airflow -c "DELETE FROM dag_run;"

# Reactivar DAGs de producción (excluir tests)
docker exec -it airflow_postgres_dedicated psql -U airflow -c "UPDATE dag SET is_paused = false WHERE dag_id NOT IN ('0_certificacion_entorno', 'dag_test', 'test_conexion_mariadb', 'test_conexion_mssql244');"

# Verificar count final
docker exec -it airflow_postgres_dedicated psql -U airflow -c "SELECT COUNT(*) FROM dag_run;"
```
> ⚠️ Ver `CONVENTIONS.md` — orden correcto para limpiar sin disparar catchup

---

## Notas

| Tema | Detalle |
|---|---|
| Driver ODBC en Anaconda | Solo veía Driver 17 — se instaló Driver 18 desde Microsoft Learn |
| Driver ODBC en Docker | Solo tiene Driver 18 — paridad lograda |
| Descarga Driver 18 | https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server |
| Schedule dag_masterclients | Cambiado de `0 6 * * 1` (todos los lunes) a `0 6 * * 1#1` (primer lunes del mes) |
| Scripts utilitarios | `scripts/db_utils/` — laboratorio previo a producción, no afecta DAGs |
