# ETL Bronze → Gold — Documentación del Proyecto

**Proyecto:** Migración ETL SSIS → Apache Airflow  
**Autor:** Andrés — Gusacapital  
**Fecha inicio:** Mayo 2026  
**Última actualización:** 22/06/2026  
**Estado:** ✅ Homologación v2 completada — clients, phones y gold

---

## Resumen

Migración de 11 pipelines ETL desde **Microsoft SSIS** hacia **Apache Airflow 2.9.3** (Docker), conectando **MariaDB 242/240** como origen y **SQL Server 244** como destino, con arquitectura de 3 capas: Bronze Clients → Bronze Phones → Gold.

---

## Arquitectura

### As-Is — Microsoft SSIS

```
pkgETL_MAESTRO_DW.dtsx
    │
    ├──► pkgClients42.dtsx        ← Clientes servidor 242
    │         ├── Flyback MAX      → Insert Flyback
    │         ├── BuyBack MAX      → Insert BuyBack
    │         └── MasterLink MAX   → Insert MasterLink
    │
    ├──► pkgClients40.dtsx        ← Clientes servidor 240
    │         ├── Vacation C MAX   → Insert Vacation C
    │         └── Financiamiento   → Insert Financiamiento
    │
    └──► pkgPhone.dtsx            ← Teléfonos todos los productos
              ├── Limpieza + Insert Phone FB
              ├── Limpieza + Insert Phone BuyBack
              ├── Limpieza + Insert Phone MasterLink
              ├── Limpieza + Insert Phone Financiamiento
              └── Limpieza + Insert Phone Vacation Center
```

### To-Be — Apache Airflow

```
dag_masterclients   ← Bronze Clients (INCREMENTAL — clientid > max_id)
    ├── dag_clientsfi_240   ✅ v2
    ├── dag_clientsvc_240   ✅ v2
    ├── dag_clientsfb_242   ✅ v2
    ├── dag_clientsbb_242   ✅ v2  ← estándar de oro
    └── dag_clientsml_242   ✅ v2

dag_masterphones    ← Bronze Phones (TRUNCATE + INSERT)
    ├── dag_phonefi_240     ✅ v2.1
    ├── dag_phonevc_240     ✅ v2.1
    ├── dag_phonefb_242     ✅ v2.1
    ├── dag_phonebb_242     ✅ v2.1
    └── dag_phoneml_242     ✅ v2.1

dag_master_gold     ← Gold Layer (SPs SQL Server) ✅ v2.2
    ├── sp_etl_maestro
    └── sp_insert_phones_factPersonalInfo
```

---

## Patrón v2 — Clientes (INCREMENTAL)

Todo DAG de clients sigue este patrón desde `dag_clientsbb_242.py` (19/06/2026).

### Estructura de archivos

```
dags/
├── etl/
│   └── dag_clientsXX_XXX.py        ← DAG — solo orquestación
├── sql/
│   └── clients/
│       ├── get_max_id.sql           ← SELECT MAX(clientid) externo
│       ├── select_clientsXX_XXX.sql ← SELECT origen con {max_id}
│       ├── insert_clientsXX_XXX.sql ← INSERT destino con %s
│       ├── exec_sp_maestro.sql      ← EXEC SP Gold Layer
│       └── exec_sp_phones.sql       ← EXEC SP Gold Layer phones
└── common/
    ├── etl_base.py      v2.4        ← Motor ETL — cero SQL embebido
    ├── audit_logger.py              ← Log a MariaDB + archivo .txt
    ├── db_connections.py            ← Connection IDs centralizados
    └── sql_loader.py                ← Carga archivos .sql externos
```

### Reglas del patrón clients

- **Cero SQL en Python** — todo SQL vive en archivos `.sql` externos
- **`.bk` obligatorio** antes de modificar cualquier archivo
- **`dag_id` como primer parámetro** en `ejecutar_insert()` para trazabilidad
- **`BATCH_SIZE = 1000`** — lotes de 1000 filas con `executemany`
- **`estado = "ERROR"` por defecto** — solo cambia a `"SUCCESS"` si completa
- **`finally` siempre loguea** — Airflow nunca se queda sin auditoría

### Firma de etl_base.py v2.4

```python
# get_max_id — SQL externo
get_max_id(mssql_conn_id, tabla_destino)
    → lee: sql/clients/get_max_id.sql

# ejecutar_insert — motor batch incremental
ejecutar_insert(
    dag_id          : str
  , mariadb_conn_id : str
  , mssql_conn_id   : str
  , sql_select      : str   # ruta relativa al .sql de SELECT
  , sql_insert      : str   # ruta relativa al .sql de INSERT
  , max_id          : int
  , etl_fecha       : datetime = None
) -> int  # filas insertadas
```

---

## Patrón v2.1 — Phones (TRUNCATE + INSERT)

Todo DAG de phones sigue este patrón desde 22/06/2026.

### Estructura de archivos

```
dags/
├── etl/
│   └── dag_phoneXX_XXX.py          ← DAG — solo orquestación
├── sql/
│   └── phones/
│       ├── select_phone.sql         ← SELECT clientid, PHONE con {vista_origen}
│       ├── insert_phone.sql         ← INSERT con {tabla_destino} y %s
│       └── truncate_phone.sql       ← TRUNCATE con {tabla_destino}
└── common/
    └── etl_basephone.py  v2.3       ← Motor TRUNCATE+INSERT
```

### Reglas del patrón phones

- **TRUNCATE + INSERT** — tabla completa en cada ejecución
- **Transacción única** — commit al final, rollback si falla en cualquier lote
- **Un solo SQL select/insert/truncate compartido** — mismo archivo para los 5 productos
- **`dag_id` como primer parámetro** — identidad en logs
- **`BATCH_SIZE = 1000`** — lotes de 1000 filas con `executemany`
- **`.bk` obligatorio** antes de modificar cualquier archivo

### Firma de etl_basephone.py v2.3

```python
ejecutar_truncate_insert(
    dag_id          : str
  , mariadb_conn_id : str
  , mssql_conn_id   : str
  , vista_origen    : str   # db_general.vwpersonalinfo{xx}
  , tabla_destino   : str   # source.Phone{xx}
) -> int  # filas insertadas
```

### Flujo de transacción

```
TRUNCATE TABLE {tabla_destino}  → commit inmediato
    ↓
SELECT clientid, PHONE FROM {vista_origen}
    ↓
executemany lote 1..N           → SIN commit por lote
    ↓
COMMIT único al final           → todo insertado o nada
    ↓ (si falla en cualquier lote)
Log: lote fallido + filas procesadas + traceback
ROLLBACK                        → tabla queda intacta
```

---

## Patrón v2.2 — Gold Layer (SQLExecuteQueryOperator)

### Regla crítica — template_searchpath obligatorio

```python
# SIEMPRE incluir en DAGs que usen SQLExecuteQueryOperator con .sql externos
with DAG(
    ...
  , template_searchpath = "/opt/airflow/dags"  # ← Jinja2 busca desde aquí
) as dag:
    tarea = SQLExecuteQueryOperator(
        sql = "sql/clients/exec_sp_maestro.sql"  # ← ruta relativa a dags/
    )
```

**Sin `template_searchpath`** Jinja2 busca el template en la carpeta del DAG
(`dags/etl/`) y lanza `TemplateNotFound`. El error es silencioso y confuso.

**Por qué no SQL embebido:**
```python
# ❌ NUNCA — SQL embebido
sql = "EXEC [dw_etl].[sp_etl_maestro]"

# ✅ SIEMPRE — SQL externo
sql = "sql/clients/exec_sp_maestro.sql"
```

### Estructura de archivos gold

```
dags/
├── etl/
│   └── dag_master_gold.py          ← DAG con template_searchpath
└── sql/
    └── clients/                    ← dominio clientes (bronze + gold)
        ├── exec_sp_maestro.sql     ← EXEC sp_etl_maestro
        └── exec_sp_phones.sql      ← EXEC sp_insert_phones_factPersonalInfo
```

---

## Conexiones Airflow

| ID | Tipo | Servidor |
|---|---|---|
| `MariaDB` | MySQL | 192.168.10.242 |
| `MariaDB240` | MySQL | 192.168.10.240 |
| `MSSQL244` | MSSQL | 192.168.10.244 |

---

## Historial de versiones

### etl_base.py (clients)

| Versión | Fecha | Cambio |
|---|---|---|
| v1.0 | May 2026 | Motor inicial — conexiones directas pymssql |
| v2.0 | May 2026 | Hooks de Airflow — sin credenciales en código |
| v2.1 | Jun 2026 | NULL → None para compatibilidad pymssql |
| v2.2 | 19/06/2026 | dag_id + traceback + blindaje conexiones finally |
| v2.3 | 22/06/2026 | get_max_id → SQL externo, cero SQL embebido |
| v2.4 | 22/06/2026 | Blindaje conexiones + log detallado en except |

### etl_basephone.py (phones)

| Versión | Fecha | Cambio |
|---|---|---|
| v1.0 | May 2026 | Motor inicial — execute row-by-row, SQL embebido |
| v2.0 | 22/06/2026 | executemany + SQL externo + dag_id + traceback |
| v2.1 | 22/06/2026 | Transacción única — commit al final, rollback si falla |
| v2.2 | 22/06/2026 | Log detallado ANTES del rollback |
| v2.3 | 22/06/2026 | TRUNCATE → SQL externo + rollback seguro con None check |

### dag_master_gold.py

| Versión | Fecha | Cambio |
|---|---|---|
| v1.0 | May 2026 | MsSqlOperator — SQL embebido |
| v2.0 | 22/06/2026 | SQLExecuteQueryOperator + SQL externo |
| v2.1 | 22/06/2026 | template_searchpath corregido |
| v2.2 | 22/06/2026 | template_searchpath = "/opt/airflow/dags" — definitivo |

---

## Auditoría de sincronización — 22/06/2026

### Clients

| DAG | MAX origen | MAX destino | Registros insertados | Estado |
|---|---|---|---|---|
| dag_clientsvc_240 | 57,765 | 57,765 | 48 | ✅ |
| dag_clientsfb_242 | ~373,927 | ~373,924 | 906 | ✅ |
| dag_clientsml_242 | 797 | 797 | 0 | ✅ |

**Nota:** Se eliminaron 111 registros con clientid `999190748–999190858` de `source.clientsfb` — carga de prueba del 15/06/2026.

### Phones

| DAG | Origen | Destino | Estado |
|---|---|---|---|
| dag_phonefb_242 | 473,843 | 473,905 | ✅ |
| dag_phonebb_242 | 11,340 | 11,340 | ✅ |
| dag_phoneml_242 | 862 | 862 | ✅ |
| dag_phonefi_240 | 94,597 | 94,604 | ✅ |
| dag_phonevc_240 | 93,977 | 93,983 | ✅ |

### Gold

| DAG | Duración | Estado |
|---|---|---|
| dag_master_gold | 15 seg | ✅ |

---

## Pendientes

- [ ] Profiling `time.perf_counter()` en `etl_basephone.py` — mañana
- [ ] Migrar `pymssql` → `pyodbc` + `fast_executemany` si profiling confirma cuello en INSERT
- [ ] Investigar 5 huérfanos en `source.clientsfb` (clientid <= 372,993)
- [ ] Investigar 3 huérfanos en `source.clientsml` (clientid <= 797)
- [ ] Refactor batch — dag_factory + BatchResult (discutir con Gemini)
- [ ] UPSERT phones cuando origen tenga llave natural por teléfono

---

## Mejoras implementadas vs SSIS

- Auditoría triple: Airflow UI + `flybackDW.etl_audit_log` + archivos `.txt`
- Reintentos automáticos: 3 intentos con 60 segundos de pausa
- DAGs individuales ejecutables ante fallos parciales
- Monitoreo desde UI web sin acceso al servidor
- SQL externalizado — modificable sin tocar Python
- Transacción única con rollback en phones — tabla nunca queda a medias
- `template_searchpath` documentado — regla de oro para Gold Layer
- Versionable en Git con historial de `.bk`
- Open Source — sin licencias adicionales
