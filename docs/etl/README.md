# ETL Bronze → Gold — Documentación del Proyecto

**Proyecto:** Migración ETL SSIS → Apache Airflow  
**Autor:** Andrés — Gusacapital  
**Fecha inicio:** Mayo 2026  
**Última actualización:** 22/06/2026  
**Estado:** 🔄 EN CURSO — Homologación v2 completada, phones pendiente

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
    ├── dag_phonefi_240     ⏳ pendiente homologación
    ├── dag_phonevc_240     ⏳ pendiente homologación
    ├── dag_phonefb_242     ⏳ pendiente homologación
    ├── dag_phonebb_242     ⏳ pendiente homologación
    └── dag_phoneml_242     ⏳ pendiente homologación

dag_master_gold     ← Gold Layer (SPs SQL Server)
    ├── sp_etl_maestro
    └── sp_insert_phones_factPersonalInfo
```

---

## Patrón v2 — Estándar de Oro

Todo DAG de clients sigue este patrón desde `dag_clientsbb_242.py` (sabado 19/06/2026).

### Estructura de archivos por DAG

```
dags/
├── etl/
│   └── dag_clientsXX_XXX.py        ← DAG — solo orquestación
├── sql/
│   └── clients/
│       ├── get_max_id.sql           ← SELECT MAX(clientid) externo
│       ├── select_clientsXX_XXX.sql ← SELECT origen con {max_id}
│       └── insert_clientsXX_XXX.sql ← INSERT destino con %s
└── common/
    ├── etl_base.py      v2.3        ← Motor ETL — cero SQL embebido
    ├── audit_logger.py              ← Log a MariaDB + archivo .txt
    ├── db_connections.py            ← Connection IDs centralizados
    └── sql_loader.py                ← Carga archivos .sql externos
```

### Reglas del patrón

- **Cero SQL en Python** — todo SQL vive en archivos `.sql` externos
- **`.bk` obligatorio** antes de modificar cualquier archivo
- **`dag_id` como primer parámetro** en `ejecutar_insert()` para trazabilidad
- **`BATCH_SIZE = 1000`** — lotes de 1000 filas con `executemany`
- **`estado = "ERROR"` por defecto** — solo cambia a `"SUCCESS"` si completa
- **`finally` siempre loguea** — Airflow nunca se queda sin auditoría

### Firma de etl_base.py v2.3

```python
# get_max_id — SQL externo
get_max_id(mssql_conn_id, tabla_destino)
    → lee: sql/clients/get_max_id.sql

# ejecutar_insert — motor batch
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

### Conexiones Airflow

| ID | Tipo | Servidor |
|---|---|---|
| `MariaDB` | MySQL | 192.168.10.242 |
| `MariaDB240` | MySQL | 192.168.10.240 |
| `MSSQL244` | MSSQL | 192.168.10.244 |

---

## Historial de versiones — etl_base.py

| Versión | Fecha | Cambio |
|---|---|---|
| v1.0 | May 2026 | Motor inicial — conexiones directas pymssql |
| v2.0 | May 2026 | Hooks de Airflow — sin credenciales en código |
| v2.1 | Jun 2026 | NULL → None para compatibilidad pymssql |
| v2.2 | 19/06/2026 | dag_id + traceback + blindaje conexiones finally |
| v2.3 | 22/06/2026 | get_max_id → SQL externo, cero SQL embebido en Python |

---

## Auditoría de sincronización — 22/06/2026

| DAG | MAX origen | MAX destino | Registros insertados | Estado |
|---|---|---|---|---|
| dag_clientsvc_240 | 57,765 | 57,765 | 48 | ✅ |
| dag_clientsfb_242 | ~373,927 | ~373,924 | 906 | ✅ |
| dag_clientsml_242 | 797 | 797 | 0 | ✅ |

**Nota:** Se eliminaron 111 registros con clientid `999190748–999190858` de `source.clientsfb` — carga de prueba del 15/06/2026.

---

## Pendientes

- [ ] Homologar DAGs phones a v2 (`etl_basephone.py`)
- [ ] Investigar 5 huérfanos en `source.clientsfb` (clientid <= 372,993)
- [ ] Investigar 3 huérfanos en `source.clientsml` (clientid <= 797)
- [ ] Refactor batch — dag_factory + BatchResult (discutir con Gemini)
- [ ] Tabla histórica de redeems desde 2013

---

## Mejoras implementadas vs SSIS

- Auditoría triple: Airflow UI + `flybackDW.etl_audit_log` + archivos `.txt`
- Reintentos automáticos: 3 intentos con 60 segundos de pausa
- DAGs individuales ejecutables ante fallos parciales
- Monitoreo desde UI web sin acceso al servidor
- SQL externalizado — modificable sin tocar Python
- Versionable en Git con historial de `.bk`
- Open Source — sin licencias adicionales
