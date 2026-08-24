# 📖 Data Dictionary — dag_clientsfb_242

**DAG:** `dag_clientsfb_242`  
**Objetivo:** ETL incremental clientes FB desde MariaDB 242 → SQL Server Silver  
**Origen:** `MariaDB 192.168.10.242` — `db_general.viewclientsfb`  
**Destino:** `SQL Server 192.168.10.244` — `source.clientsfb`  
**Patrón:** Incremental por `clientid > max_id`  
**Última actualización:** 2026-08-21  

---

## Flujo ETL

```
MariaDB 242
db_general.viewclientsfb
        │
        │  SELECT 20 columnas
        │  WHERE clientid > {max_id}
        │  CAST(capdata AS DATE)
        ▼
etl_base.py
        │  + createdAt = NOW()   ← fecha de inserción ETL
        │  + updatedAt = NULL    ← pendiente v2
        │  + deletedAt = NULL    ← pendiente v2
        ▼
SQL Server 244
source.clientsfb
(23 columnas total)
```

---

## Contrato de Columnas

| # | Columna | Tipo Origen (MariaDB) | Transformación ETL | Tipo Destino (SQL Server) | Notas |
|---|---|---|---|---|---|
| 1 | `productid` | `int(1)` NOT NULL | — | `int` NOT NULL | ID del producto |
| 2 | `contractid` | `varchar(50)` | — | `nvarchar(150)` | ID del contrato |
| 3 | `clientid` | `int(12)` NOT NULL | — | `int` NOT NULL | **PK incremental** — clave de carga |
| 4 | `email` | `mediumtext` | — | `nvarchar(255)` | Email del cliente |
| 5 | `capdata` | `datetime` | `CAST(capdata AS DATE)` | `date` | Fecha de capacitación — se elimina la hora |
| 6 | `FirstName` | `varchar(50)` | — | `nvarchar(255)` | Nombre |
| 7 | `LastName` | `varchar(50)` | — | `nvarchar(255)` | Apellido |
| 8 | `countrycode` | `varchar(255)` | — | `nvarchar(255)` | Código de país |
| 9 | `country` | `varchar(255)` | — | `nvarchar(300)` | País |
| 10 | `Estate` | `varchar(100)` | — | `nvarchar(1000)` | Estado / Provincia |
| 11 | `Ciudad` | `varchar(100)` | — | `nvarchar(300)` | Ciudad |
| 12 | `address` | `text` | — | `nvarchar(MAX)` | Dirección completa |
| 13 | `zip` | `varchar(16)` | — | `nvarchar(20)` | Código postal |
| 14 | `Corpcode` | `varchar(12)` | — | `nvarchar(22)` | Código corporativo |
| 15 | `Corp` | `varchar(30)` | — | `nvarchar(100)` | Nombre corporativo — ampliado por valores largos |
| 16 | `ingreso` | `int(1)` NOT NULL | — | `decimal(18,0)` | Fecha ingreso numérica |
| 17 | `egreso` | `int(1)` NOT NULL | — | `int` | Fecha egreso numérica |
| 18 | `rank` | `int(1)` NOT NULL | — | `int` | Ranking del cliente |
| 19 | `EstatusN` | `decimal(5,3)` NOT NULL | — | `decimal(11,3)` | Estatus numérico — ampliado 2026-08-19 |
| 20 | `EstatusL` | `varchar(64)` | — | `nvarchar(64)` | Estatus literal — ampliado 2026-08-20 (era 20) |
| — | `createdAt` | *(no viene del origen)* | `NOW()` en etl_base.py | `datetime` | Fecha de inserción ETL en Silver |
| — | `updatedAt` | *(no viene del origen)* | `NULL` en etl_base.py | `datetime2` | Pendiente v2 — detección de cambios |
| — | `deletedAt` | *(no viene del origen)* | `NULL` en etl_base.py | `nvarchar(255)` | Pendiente v2 — detección de borrados |

---

## Archivos SQL

| Archivo | Ruta | Descripción |
|---|---|---|
| SELECT | `dags/sql/clients/select_clientsfb_242.sql` | 20 columnas + CAST(capdata AS DATE) |
| INSERT | `dags/sql/clients/insert_clientsfb_242.sql` | 23 columnas con placeholders `?` |

---

## Historial de cambios

| Fecha | Versión | Cambio |
|---|---|---|
| 2026-06-19 | v1.0 | Creación inicial del DAG |
| 2026-08-19 | v2.0 | SQL externalizado + executemany |
| 2026-08-19 | v2.1 | `EstatusN` → `decimal(11,3)` en destino |
| 2026-08-19 | v2.2 | SELECT corregido a 20 columnas — etl_base agrega auditoría |
| 2026-08-19 | v2.3 | `CAST(capdata AS DATE)` — compatibilidad SQL Server |
| 2026-08-20 | v2.4 | `Corp` → `nvarchar(100)`, `EstatusL` → `nvarchar(64)`, `zip` → `nvarchar(20)` |

---

> **Nota de arquitectura:** Las columnas `createdAt`, `updatedAt` y `deletedAt` **no se seleccionan desde la vista origen**. Son agregadas automáticamente por `etl_base.py` en cada inserción. `createdAt` registra el momento exacto del ETL. `updatedAt` y `deletedAt` quedan en NULL hasta que se implemente la lógica de detección de cambios en v2.
