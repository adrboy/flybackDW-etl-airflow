# ETL Bronze Phones — Documentación

**Proyecto:** Migración ETL SSIS → Apache Airflow — Capa Phones  
**Autor:** Andrés — Gusacapital  
**Fecha inicio:** Mayo 2026  
**Última actualización:** 22/06/2026  
**Estado:** ✅ v2.2 — TRUNCATE+INSERT con transacción atómica

---

## Resumen

5 pipelines ETL de teléfonos desde **MariaDB 242/240** hacia **SQL Server 244**.  
Patrón: **TRUNCATE + INSERT completo** en cada ejecución — motor `etl_basephone.py`.  
A diferencia de los DAGs de clients (INCREMENTAL), phones recarga la tabla completa cada vez porque un cliente puede tener múltiples teléfonos y no existe llave natural por teléfono en el origen.

---

## DAGs

| DAG | Origen | Servidor | Destino | Registros (~) |
|---|---|---|---|---|
| `dag_phonebb_242` | `db_general.vwpersonalinfobb` | MariaDB 242 | `source.Phonebb` | 11,340 |
| `dag_phonefb_242` | `db_general.vwpersonalinfofb` | MariaDB 242 | `source.Phonefb` | 473,905 |
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
│   ├── dag_phonebb_242.py.bk  ← respaldo v1 row-by-row
│   ├── dag_phonefb_242.py.bk
│   ├── dag_phoneml_242.py.bk
│   ├── dag_phonefi_240.py.bk
│   └── dag_phonevc_240.py.bk
├── sql/
│   └── phones/
│       ├── select_phone.sql   ← SELECT clientid, PHONE con {vista_origen}
│       └── insert_phone.sql   ← INSERT con {tabla_destino} y %s
└── common/
    ├── etl_basephone.py  v2.2 ← motor reutilizable — los 5 DAGs lo usan
    └── etl_basephone.py.bk   ← respaldo v1 row-by-row para referencia
```

---

## Motor — etl_basephone.py

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

### Flujo de transacción

```
TRUNCATE TABLE {tabla_destino}     → commit inmediato (libera espacio)
    ↓
SELECT clientid, PHONE             → cursor MariaDB
    ↓
while fetchmany(1000):
    executemany(lote)              → SIN commit por lote
    ↓
COMMIT único al final              → todo o nada ✅
    ↓ (si falla en cualquier lote)
Log: lote fallido + filas procesadas + traceback
ROLLBACK                           → tabla queda intacta ✅
```

### Reglas del motor

- **Cero SQL en Python** — todo SQL en `sql/phones/*.sql`
- **`BATCH_SIZE = 1000`** — lotes de 1000 filas con `executemany`
- **Transacción única** — commit al final, nunca tabla a medias
- **Log antes del rollback** — diagnóstico completo antes de revertir
- **`dag_id` en cada print** — trazabilidad en logs de Airflow
- **`finally` cierra conexiones** — siempre, pase lo que pase

---

## Modelo de datos

### Origen MariaDB (todas las vistas)

| Columna | Tipo | Nota |
|---|---|---|
| `clientid` | int | FK al cliente — puede repetirse (1 cliente N teléfonos) |
| `PHONE` | varchar(22) | Número de teléfono |

### Destino SQL Server (todas las tablas Phone*)

| Columna | Tipo | Nota |
|---|---|---|
| `idphone` | int | PK identity — generada por SQL Server |
| `clientid` | int | Nullable — puede repetirse |
| `phone` | varchar | Número de teléfono |
| `atInsert` | datetime | Fecha de carga ETL |
| `atUpdate` | datetime | NULL — TRUNCATE+INSERT, no hay update |

**Nota:** `Phonefb` usa `smalldatetime` para `atInsert/atUpdate`. Las demás usan `datetime`.

---

## Por qué TRUNCATE+INSERT y no INCREMENTAL

El origen solo tiene `clientid` + `PHONE` — sin llave natural por teléfono. Un cliente puede tener hasta 7 teléfonos distintos en la misma tabla. Sin una llave que identifique un teléfono específico de un cliente, no es posible hacer UPSERT confiable.

**Pendiente v3.0:** Cuando el origen agregue una llave natural por teléfono, migrar a UPSERT incremental para eliminar el TRUNCATE.

---

## Historial de versiones — etl_basephone.py

| Versión | Fecha | Cambio |
|---|---|---|
| v1.0 | May 2026 | Motor inicial — execute row-by-row, SQL embebido, commit por lote |
| v2.0 | 22/06/2026 | executemany + SQL externo + dag_id + traceback |
| v2.1 | 22/06/2026 | Transacción única — commit al final, rollback si falla |
| v2.2 | 22/06/2026 | Log detallado ANTES del rollback: lote, filas, traceback, confirmación |

---

## Benchmark y performance

### Resultados actuales — dag_phonefb_242 (473k registros)

| Fecha | Driver | Filas | FETCH (s) | INSERT (s) | RPS | Total |
|---|---|---|---|---|---|---|
| 28/05/2026 | pymssql row-by-row v1 | 470,926 | ⏱ | ⏱ | ⏱ | 11m 51s |
| 22/06/2026 | pymssql executemany v2 | 473,905 | ⏱ | ⏱ | ⏱ | 12m 23s |
| pendiente | pymssql + profiling | — | ✍ | ✍ | ✍ baseline |
| futuro | pyodbc fast_executemany | — | ✍ | ✍ | 🚀 | — |

### Plan de optimización (próxima sesión)

1. **Profiling** — `time.perf_counter()` para `tiempo_fetch` vs `tiempo_insert` por lote
2. **RPS** — `rows_per_second = filas / tiempo_insert` como métrica baseline
3. **Driver** — si INSERT es el cuello → migrar a `pyodbc` + `fast_executemany = True`
4. **Dockerfile** — instalar `msodbcsql18` + `unixodbc-dev` para `pyodbc`

### Hipótesis

`pymssql` emula `executemany` internamente como inserts individuales — por eso no hubo mejora al cambiar de `execute` a `executemany`. El profiling lo confirmará.

---

## Auditoría — 22/06/2026

| DAG | Origen | Destino | Duración | Estado |
|---|---|---|---|---|
| dag_phonefb_242 | 473,843 | 473,905 | 12m 23s | ✅ |
| dag_phonebb_242 | 11,340 | 11,340 | 24s | ✅ |
| dag_phoneml_242 | 862 | 862 | 5s | ✅ |
| dag_phonefi_240 | 94,597 | 94,604 | ~2m 30s | ✅ |
| dag_phonevc_240 | 93,977 | 93,983 | ~2m 30s | ✅ |

*Diferencias origen/destino = registros nuevos en vivo durante la ejecución.*
