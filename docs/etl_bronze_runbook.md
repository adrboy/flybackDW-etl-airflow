# 🔧 Runbook — ETL Pipeline Bronze → Gold

**Proyecto:** flybackDW ETL Pipeline  
**Versión:** 2.0  
**Autor:** Andrés  
**Fecha:** Mayo 2026

---

## 📋 Descripción General

Este runbook describe cómo operar, monitorear y solucionar problemas del ETL Pipeline que carga datos desde MariaDB hacia SQL Server usando Apache Airflow en Docker.

---

## 🗺️ Mapa de DAGs

```
dag_masterclients (lunes 6am)
    ├── dag_clientsfi_240
    ├── dag_clientsvc_240
    ├── dag_clientsfb_242
    ├── dag_clientsbb_242
    └── dag_clientsml_242

dag_masterphones (lunes 6am)
    ├── dag_phonefi_240
    ├── dag_phonevc_240
    ├── dag_phonefb_242
    ├── dag_phonebb_242
    └── dag_phoneml_242

dag_master_gold (lunes 7am)
    ├── sp_etl_maestro
    └── sp_insert_phones_factPersonalInfo
```

---

## 🚀 Operación Normal

### Verificar contenedores
```bash
docker-compose ps
```

### Verificar runs de un DAG
```bash
docker-compose exec airflow-scheduler airflow dags list-runs -d dag_masterclients
```

### Buscar logs de un DAG
```bash
docker-compose exec airflow-scheduler find /opt/airflow/logs -name "*.log" -path "*clientsfb*"
```

### Leer log de un run
```bash
docker-compose exec airflow-scheduler cat "/opt/airflow/logs/dag_id=dag_clientsfb_242/run_id=<run_id>/task_id=etl_clientsfb/attempt=1.log"
```

### Verificar archivo guardado en Docker
```bash
docker-compose exec airflow-scheduler wc -l /opt/airflow/dags/etl/dag_clientsfb_242.py
```

---

## 🔴 Solución de Problemas

### ⚠️ Lección aprendida — Conexiones hardcodeadas vs Hook de Airflow

Durante el desarrollo inicial se usaron credenciales hardcodeadas directamente en el código Python usando `pymysql.connect()` con usuario/password explícitos. Esto causó errores de `Access Denied` porque el contenedor Docker se conecta desde una IP diferente (`192.168.10.54`) a la que tenía permisos el usuario en MariaDB.

**Solución implementada:** Centralizar todas las conexiones en la UI de Airflow y usar `MySqlHook` en lugar de `pymysql.connect()` directo:

```python
# ❌ Forma inicial (hardcoded - solo para pruebas)
conn = pymysql.connect(host="192.168.10.242", user="andres", password="***")

# ✅ Forma correcta (Hook de Airflow)
from airflow.hooks.mysql_hook import MySqlHook
hook = MySqlHook(mysql_conn_id='MariaDB')
conn = hook.get_conn()
```

> 🔑 **Regla:** Nunca hardcodear credenciales en código productivo. Siempre usar las conexiones configuradas en Airflow UI (`Admin → Connections`).

---

### Error: Access Denied MariaDB
```
pymysql.err.OperationalError: (1045, "Access denied for user...")
```
**Causa:** El usuario no tiene permisos desde la IP del contenedor.  
**Solución:** Pedir al admin de MariaDB:
```sql
GRANT SELECT ON db_general.* TO 'andres'@'192.168.10.%';
FLUSH PRIVILEGES;
```

---

### Error: Cannot import name from common
```
ImportError: cannot import name 'get_max_id' from 'common.etl_base'
```
**Causa:** Archivo no guardado (Ctrl+S olvidado).  
**Solución:**
```bash
docker-compose exec airflow-scheduler wc -l /opt/airflow/dags/common/etl_base.py
# Debe tener más de 50 líneas
```

---

### Error: DAG no aparece en la UI
**Causa:** Archivo no guardado o error de sintaxis.  
**Solución:**
1. Verificar punto blanco en pestaña VSCode
2. Esperar 30 segundos
3. Ctrl+Shift+R en el navegador

---

### Error: ExternalTaskSensor timeout
```
ExternalTaskSensor timed out
```
**Causa:** El sensor busca un run del mismo `execution_date` y no lo encuentra.  
**Solución temporal:** Ejecutar `dag_master_gold` manualmente sin sensor.  
**Solución permanente:** Agregar `execution_delta=timedelta(hours=24)` al sensor.

---

## 📊 Verificación Post-Ejecución

### Clients Bronze
```sql
SELECT COUNT(*), MAX(clientid) FROM DBGeneralDW.source.clientsfb
SELECT COUNT(*), MAX(clientid) FROM DBGeneralDW.source.clientsbb
SELECT COUNT(*), MAX(clientid) FROM DBGeneralDW.source.clientsml
SELECT COUNT(*), MAX(clientid) FROM DBGeneralDW.source.clientsfi
SELECT COUNT(*), MAX(clientid) FROM DBGeneralDW.source.clientsvc
```

### Phones Bronze
```sql
SELECT COUNT(*) FROM DBGeneralDW.source.Phonefb
SELECT COUNT(*) FROM DBGeneralDW.source.Phonebb
SELECT COUNT(*) FROM DBGeneralDW.source.Phoneml
SELECT COUNT(*) FROM DBGeneralDW.source.Phonefi
SELECT COUNT(*) FROM DBGeneralDW.source.Phonevc
```

### Gold
```sql
SELECT COUNT(*) FROM gral.factClientes
SELECT COUNT(*) FROM gral.factClientesDetalle
SELECT COUNT(*) FROM gral.factPersonalInfo
```

### Auditoría Airflow
```sql
SELECT * FROM flybackDW.etl_audit_log ORDER BY fecha_inicio DESC
```

### Auditoría SQL Server
```sql
SELECT * FROM dw_etl.dw_etl_audit_log ORDER BY fecha_ejecucion DESC
```

---

## 🔄 Reiniciar Contenedores

```bash
# Solo cuando se modifica docker-compose.yml
docker-compose down
docker-compose up -d
docker-compose logs -f airflow-webserver
```

> ⚠️ Para cambios en DAGs NO es necesario reiniciar.

---

## 📅 Schedule de Ejecución

| DAG | Schedule | Descripción |
|---|---|---|
| `dag_masterclients` | `0 6 * * 1` | Lunes 6am — Bronze Clients |
| `dag_masterphones` | `0 6 * * 1` | Lunes 6am — Bronze Phones |
| `dag_master_gold` | `0 7 * * 1` | Lunes 7am — Gold Layer |

---

## 🔌 Conexiones en Airflow

| Conn Id | Tipo | Host | Puerto |
|---|---|---|---|
| `MariaDB` | MySQL | `192.168.10.242` | `3306` |
| `MariaDB240` | MySQL | `192.168.10.240` | `3306` |
| `MSSQL244` | Microsoft SQL Server | `192.168.10.244` | `1433` |

---

## 📦 Estructura de Archivos

```
DockersETL/
├── docker-compose.yml
├── .env
└── dags/
    ├── common/
    │   ├── audit_logger.py      ← logs tabla + archivo .txt
    │   ├── db_connections.py    ← conexiones centralizadas
    │   ├── etl_base.py          ← lógica incremental clients
    │   └── etl_basephone.py     ← lógica TRUNCATE+INSERT phones
    └── etl/
        ├── dag_clientsfb_242.py
        ├── dag_clientsbb_242.py
        ├── dag_clientsml_242.py
        ├── dag_clientsfi_240.py
        ├── dag_clientsvc_240.py
        ├── dag_masterclients.py
        ├── dag_phonefb_242.py
        ├── dag_phonebb_242.py
        ├── dag_phoneml_242.py
        ├── dag_phonefi_240.py
        ├── dag_phonevc_240.py
        ├── dag_masterphones.py
        └── dag_master_gold.py
```

---

*Última actualización: 28/05/2026*
