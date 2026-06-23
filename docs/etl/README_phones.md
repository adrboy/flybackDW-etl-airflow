# ETL Bronze Phones — Documentación

**Proyecto:** Migración ETL SSIS → Apache Airflow — Capa Phones  
**Autor:** Andrés — Gusacapital  
**Fecha inicio:** Mayo 2026  
**Última actualización:** 23/06/2026  
**Estado:** ✅ v3.0 — pyodbc + fast_executemany — 73x más rápido

---

## Resumen

5 pipelines ETL de teléfonos desde **MariaDB 242/240** hacia **SQL Server 244**.  
Patrón: **TRUNCATE + INSERT completo** en cada ejecución — motor `etl_basephone.py`.  
Driver: **pyodbc + fast_executemany = True** con msodbcsql18 para SQL Server 2022.

---

## DAGs

| DAG | Origen | Servidor | Destino | Registros (~) |
|---|---|---|---|---|
| `dag_phonebb_242` | `db_general.vwpersonalinfobb` | MariaDB 242 | `source.Phonebb` | 11,340 |
| `dag_phonefb_242` | `db_general.vwpersonalinfofb` | MariaDB 242 | `source.Phonefb` | 474,137 |
| `dag_phoneml_242` | `db_general.vwpersonalinfoml` | MariaDB 242 | `source.Phoneml` | 862 |
| `dag_phonefi_240` | `db_general.vwpersonalinfofi` | MariaDB 240 | `source.Phonefi` | 94,604 |
| `dag_phonevc_240` | `db_general.vwpersonalinfovc` | MariaDB 240 | `source.Phonevc` | 93,983 |

---

## Estructura de archivos

```
dags/
├── etl/
│   ├── dag_phonebb_242.py     ✅ v2.1
│   ├── dag_phonefb_242.py     ✅ v2.1
│   ├── dag_phoneml_242.py     ✅ v2.1
│   ├── dag_phonefi_240.py     ✅ v2.1
│   ├── dag_phonevc_240.py     ✅ v2.1
│   └── *.py.bk                ← respaldos v1
├── sql/
│   └── phones/
│       ├── select_phone.sql   ← SELECT clientid, PHONE con {vista_origen}
│       ├── insert_phone.sql   ← INSERT con {tabla_destino} y ? (pyodbc)
│       └── truncate_phone.sql ← TRUNCATE con {tabla_destino}
└── common/
    ├── etl_basephone.py  v3.0 ← pyodbc + fast_executemany
    └── etl_basephone.py.bk   ← respaldo pymssql
```

---

## Motor — etl_basephone.py v3.0

### Firma

```python
ejecutar_truncate_insert(
    dag_id          : str   # identidad en logs
  , mariadb_conn_id : str   # ID conexión Airflow → MariaDB origen
  , mssql_conn_id   : str   # ID conexión Airflow → SQL Server destino
  , vista_origen    : str   # db_general.vwpersonalinfo{xx}
  , tabla_destino   : str   # source.Phone{xx}
) -> int                    # filas insertadas
```

### Conexión pyodbc

```python
conn_data = BaseHook.get_connection(mssql_conn_id)
conn_str  = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={conn_data.host};"
    f"DATABASE={conn_data.schema};"
    f"UID={conn_data.login};"
    f"PWD={conn_data.password};"
    f"TrustServerCertificate=yes;"
)
conn_destino = pyodbc.connect(conn_str)
conn_destino.autocommit = False
cursor_destino.fast_executemany = True  # ← clave del rendimiento
```

### Flujo de transacción

```
TRUNCATE TABLE {tabla_destino}     → commit inmediato
    ↓
SELECT clientid, PHONE             → cursor MariaDB
    ↓
while fetchmany(1000):
    fast_executemany(lote)         → SIN commit por lote
    ↓
COMMIT único al final              → todo o nada ✅
    ↓ (si falla)
Log: lote + filas + traceback
ROLLBACK                           → tabla intacta ✅
```

### Reglas del motor

- **pyodbc + fast_executemany** — driver nativo SQL Server 2022
- **`?` como placeholder** — sintaxis pyodbc (no `%s` de pymssql)
- **Cero SQL en Python** — todo SQL en `sql/phones/*.sql`
- **`BATCH_SIZE = 1000`** — variable, fácil de ajustar
- **Transacción única** — commit al final, nunca tabla a medias
- **Log + JSON metrics** — diagnóstico completo en cada run
- **Credenciales seguras** — via `BaseHook.get_connection()`

---

## Modelo de datos

### Origen MariaDB

| Columna | Tipo | Nota |
|---|---|---|
| `clientid` | int | FK al cliente — puede repetirse (1 cliente N teléfonos) |
| `PHONE` | varchar(22) | Número de teléfono |

### Destino SQL Server

| Columna | Tipo | Nota |
|---|---|---|
| `idphone` | int | PK identity — generada por SQL Server |
| `clientid` | int | Nullable — puede repetirse |
| `phone` | varchar | Número de teléfono |
| `atInsert` | datetime | Fecha de carga ETL |
| `atUpdate` | datetime | NULL — TRUNCATE+INSERT |

---

## Benchmark y performance — phonefb (474k registros)

| Fecha | Driver | Filas | FETCH (s) | INSERT (s) | RPS | Total |
|---|---|---|---|---|---|---|
| 28/05/2026 | pymssql row-by-row v1 | 470,926 | — | — | — | 11m 51s |
| 22/06/2026 | pymssql executemany v2 | 473,905 | 0.02s | 695.55s | 682 | 11m 35s |
| 23/06/2026 | **pyodbc fast_executemany v3** | **474,137** | **0.02s** | **9.50s** | **49,891** | **9.59s** |

### Mejora: 73x más rápido 🚀

| Métrica | pymssql | pyodbc | Factor |
|---|---|---|---|
| INSERT | 695.55s | 9.50s | **73x** |
| RPS | 682 | 49,891 | **73x** |
| Total | 695.65s | 9.59s | **73x** |

### Diagnóstico del profiling (23/06/2026)

```
SELECT execute : 2.00s   ← latencia inicial MariaDB
FETCH  MariaDB : 0.02s   ← prácticamente instantáneo
MEM    Python  : 0.06s   ← prácticamente instantáneo
INSERT SQL Svr : 9.50s   ← cuello resuelto con pyodbc
RPS            : 49,891 rows/sec
TOTAL          : 9.59s  | Filas: 474,137
```

**Causa raíz:** `pymssql` emulaba `executemany` como inserts individuales — 474k roundtrips.
**Solución:** `pyodbc` + `fast_executemany = True` + `msodbcsql18` — bulk nativo de SQL Server.
**Analogía SSIS:** Equivalente a activar "Bulk Load" en KingswaySoft. El mismo problema, la misma solución.

---

## Historial de versiones — etl_basephone.py

| Versión | Fecha | Cambio |
|---|---|---|
| v1.0 | May 2026 | Motor inicial — execute row-by-row, SQL embebido |
| v2.0 | 22/06/2026 | executemany + SQL externo + dag_id + traceback |
| v2.1 | 22/06/2026 | Transacción única — commit al final, rollback si falla |
| v2.2 | 22/06/2026 | Log detallado ANTES del rollback |
| v2.3 | 22/06/2026 | TRUNCATE → SQL externo + rollback seguro None check |
| v2.4 | 23/06/2026 | Profiling fetch + mem + insert + RPS |
| v2.5 | 23/06/2026 | SELECT 1 sync + JSON metrics + BATCH_SIZE variable |
| **v3.0** | **23/06/2026** | **pyodbc + fast_executemany — 73x más rápido** |

---

## Infraestructura — Dockerfile

```dockerfile
FROM apache/airflow:2.9.3
USER root
RUN apt-get update \
    && apt-get install -y curl gnupg2 apt-transport-https \
    && apt-get remove -y unixodbc-dev libodbc1 unixodbc libodbcinst2 \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/12/prod.list \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir pyodbc apache-airflow-providers-odbc ...
```

**Verificación driver:**
```bash
odbcinst -q -d -n 'ODBC Driver 18 for SQL Server'
# [ODBC Driver 18 for SQL Server]
# Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.6.so.2.1
```

---

## Pendientes

- [ ] Aplicar pyodbc a `etl_base.py` (clients) — mismo patrón
- [ ] Probar `BATCH_SIZE = 5000` para ver si mejora aún más
- [ ] UPSERT cuando origen tenga llave natural por teléfono
