# etl_datasync — Sincronizador Mensual de Clientes

## ¿Qué es esto?

Migración del sincronizador `DataBaseSynchronizer` (C# / Visual Studio) a Apache Airflow.  
Consolida clientes de 4 sistemas origen en una sola tabla `db_general.complete` cada primer lunes del mes.

**DAG:** `dag_datasync_master`  
**Schedule:** `0 1 * * MON#1` — primer lunes de cada mes a la 1:00 AM  
**Duración promedio:** ~8 minutos  
**Última ejecución exitosa:** 2026-08-04

---

## Arquitectura

```
etl_datasync/
├── datasync_master.py          ← DAG — solo orquestación, sin lógica de datos
├── operations/                 ← Capa de datos — una función por operación
│   ├── gusa_collections.py     
│   ├── flyback.py              
│   ├── buyback.py              
│   ├── vacation_center.py      
│   ├── generate_data.py        
│   ├── validate_country.py     
│   └── log_estadisticas.py     
└── scripts/                    ← Ejecución manual por operación
    ├── run_gusa_collections.py
    ├── run_flyback.py
    ├── run_buyback.py
    ├── run_vacation_center.py
    ├── run_generate_data.py
    ├── run_validate_country.py
    └── run_log_estadisticas.py

common/
├── db_datasync.py              ← Motor de sincronización — MySqlHook + batch
├── db_connections.py           ← Constantes de conexiones Airflow
└── sql_loader.py               ← Cargador de archivos .sql

sql/datasync/                   ← Queries SQL externos — nunca embebidos en Python
├── select_*.sql                ← Lectura de cada origen
├── insert_*.sql                ← Escritura en db_general
├── truncate_*.sql              ← Limpieza antes de sincronizar
└── update_*.sql                ← Normalización y estadísticas
```

---

## Flujo de ejecución

```
Fase 1 — PARALELO (4 tareas independientes)
┌─────────────────────────────────────────────────┐
│  gusa_collections  240→242   ~1 min 34 seg       │
│  flyback           242→242   ~7 min 15 seg       │
│  buyback           242→242   ~18 seg             │
│  vacation_center   240→242   ~1 min 27 seg       │
└─────────────────────────────────────────────────┘
                        ↓ todas terminan
Fase 2 — SECUENCIAL
  generate_data      → db_general.complete   ~18 seg
  validate_country   → normaliza country_code ~4 seg
  log_estadisticas   → complete_details       ~1 seg
  notificar          → email + log            ~1 seg
```

---

## Servidores y bases de datos

| Conexión Airflow | Servidor | Base de datos | Equivalente C# |
|---|---|---|---|
| `MariaDB_gusa` | 192.168.10.240 | financiamiento | `dbGusa` |
| `MariaDB_flyback` | 192.168.10.242 | customers | `dbFlyBack` |
| `MariaDB_buyback` | 192.168.10.242 | buyback | `dbBuyBack` |
| `MariaDB_vtw` | 192.168.10.240 | vtw | `dbVC` |
| `MariaDB_global` | 192.168.10.242 | db_general | `dbGlobal` |

---

## Tablas involucradas

| Tabla | Rol | Se trunca |
|---|---|---|
| `db_general.gusa_collections` | Intermedia — clientes GC | Sí |
| `db_general.flyback` | Intermedia — clientes FB | Sí |
| `db_general.buyback` | Intermedia — clientes BB | Sí |
| `db_general.vtw` | Intermedia — clientes VTW | Sí |
| `db_general.complete` | Final — todos los clientes | Sí |
| `db_general.complete_details` | Histórico de runs | No — acumula |
| `db_general.update_tables` | Genera update_id por run | No — acumula |
| `db_general.log` | Log de operaciones | No — acumula |

---

## Decisiones técnicas importantes

### 1. Conexiones dedicadas por base de datos
`mysql-connector-python` conflictúa con `MySQLdb` (driver de `MySqlHook`) cuando comparten proceso en Airflow. La solución fue crear una conexión Airflow dedicada por cada BD origen — mismo patrón que el C# original (`dbGusa`, `dbFlyBack`, etc).

### 2. `use_pure=True` en mysql-connector-python
La extensión C de `mysql-connector-python` causa `Commands out of sync (2014)` en Airflow. `use_pure=True` fuerza el driver Python puro que no tiene este conflicto.

### 3. BATCH_SIZE = 10,000
`max_allowed_packet` del servidor MariaDB es 16MB. Con ~500 bytes por fila, 10,000 filas = ~5MB por lote — dentro del límite seguro. Si el servidor sube a 64MB se puede subir a 50,000 para mejor rendimiento.

### 4. fetchall() + cerrar origen antes de insertar
Con `mysql-connector-python`, el cursor de origen debe cerrarse explícitamente antes de ejecutar el INSERT en destino. Sin esto se produce `Commands out of sync` aunque sean conexiones distintas.

### 5. `GROUP_CONCAT` solo para campos `text`
En `insert_complete.sql`, los campos numéricos (`int`, `decimal`) y de fecha (`date`) usan `SUM`, `MAX` — nunca `GROUP_CONCAT`. Solo los campos `text` y `varchar` largos usan `GROUP_CONCAT`.

### 6. Schedule: primer lunes del mes
`0 1 * * MON#1` — si el día 1 cae domingo no hay nadie monitoreando. El primer lunes garantiza supervisión humana disponible.

### 7. `catchup=False`
Evita que Airflow acumule runs históricos si el DAG estuvo pausado.

---

## Ejecución manual (scripts)

Desde la terminal de VS Code, dentro del container Docker:

```powershell
cd "C:\Users\GUSA CAPITAL\Documents\DockersETL"

# Fase 1 — cualquier orden
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_gusa_collections.py
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_flyback.py
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_buyback.py
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_vacation_center.py

# Fase 2 — en orden estricto
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_generate_data.py
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_validate_country.py
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_log_estadisticas.py
```

Medir tiempo de ejecución:
```powershell
Measure-Command { docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_flyback.py }
```

---

## Monitoreo del DAG

Ver todos los runs:
```powershell
docker exec -it airflow_scheduler airflow dags list-runs -d dag_datasync_master
```

Ver estado de tasks del último run:
```powershell
docker exec -it airflow_scheduler airflow tasks states-for-dag-run dag_datasync_master "RUN_ID"
```

Ejecutar manualmente:
```powershell
docker exec -it airflow_scheduler airflow dags trigger dag_datasync_master
```

Ver logs de una task fallida:
```powershell
docker exec -it airflow_scheduler cat "/opt/airflow/logs/dag_id=dag_datasync_master/run_id=RUN_ID/task_id=TASK/attempt=1.log"
```

---

## Verificación de datos post-ejecución

```sql
-- Conteos por tabla fuente
SELECT 'gusa_collections' tabla, COUNT(*) FROM db_general.gusa_collections
UNION ALL SELECT 'flyback',       COUNT(*) FROM db_general.flyback
UNION ALL SELECT 'buyback',       COUNT(*) FROM db_general.buyback
UNION ALL SELECT 'vtw',           COUNT(*) FROM db_general.vtw
UNION ALL SELECT 'complete',      COUNT(*) FROM db_general.complete;

-- Último registro de estadísticas
SELECT * FROM db_general.complete_details ORDER BY id DESC LIMIT 3;
```

---

## Pendientes futuros

- **Procesamiento incremental** — agregar columna `updatedAt` a tablas origen para procesar solo registros nuevos/modificados en lugar de TRUNCATE + INSERT completo cada mes
- **Log robusto** — reemplazar `INSERT INTO log` con sistema de logging centralizado en `common/`
- **Refactorizar `etl/`** — aplicar el mismo patrón de dos capas y conexiones dedicadas
- **`LOAD DATA INFILE`** — para cuando los volúmenes crezcan a millones de registros

---

## Origen del proyecto

Migración de `DataBaseSynchronizer` — proyecto C# / Visual Studio que corría manualmente.  
El C# insertaba fila por fila en un `ForEach`. Python con `executemany` + `BATCH_SIZE=10,000` logra el mismo resultado en ~8 minutos de forma automática y programada.
