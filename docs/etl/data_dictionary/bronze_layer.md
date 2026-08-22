# 📖 Diccionario de Datos — Bronze & Gold Layer

**Proyecto:** flybackDW ETL Pipeline  
**Base de datos destino:** `DBGeneralDW` (SQL Server 192.168.10.244)  
**Autor:** Andrés  
**Fecha:** Mayo 2026  
**Última actualización:** 2026-08-21

---

## 📋 Modelo de Capas

```
Bronze Clients → Bronze Phones → Gold
(Incremental)   (Full Refresh)  (SPs SQL Server)
```

---

## 🥉 Bronze Layer — Clients

### Patrón de carga: Incremental por `clientid`

| # | DAG | Vista Origen (MariaDB) | Instancia | Tabla Destino | Doc |
|---|---|---|---|---|---|
| 1 | `dag_clientsfb_242` | `db_general.viewclientsfb` | 242 | `source.clientsfb` | [📄](clients/clientsfb_242.md) |
| 2 | `dag_clientsbb_242` | `db_general.viewclientsbb` | 242 | `source.clientsbb` | [📄](clients/clientsbb_242.md) |
| 3 | `dag_clientsml_242` | `db_general.viewclientsml` | 242 | `source.clientsml` | [📄](clients/clientsml_242.md) |
| 4 | `dag_clientsfi_240` | `db_general.viewclientsfi` | 240 | `source.clientsfi` | [📄](clients/clientsfi_240.md) |
| 5 | `dag_clientsvc_240` | `db_general.viewclientsvc` | 240 | `source.clientsvc` | [📄](clients/clientsvc_240.md) |

### Estructura de columnas Silver (común a las 5 tablas)

| # | Columna | Tipo Origen (MariaDB) | Transformación ETL | Tipo Destino (SQL Server) | Notas |
|---|---|---|---|---|---|
| 1 | `productid` | `int(1)` NOT NULL | — | `int` NOT NULL | ID del producto |
| 2 | `contractid` | `varchar(50)` | — | `nvarchar(150)` | ID del contrato |
| 3 | `clientid` | `int(12)` NOT NULL | — | `int` NOT NULL | **PK incremental** — clave de carga |
| 4 | `email` | `mediumtext` | — | `nvarchar(255)` | Email del cliente |
| 5 | `capdata` | `datetime` | `CAST(capdata AS DATE)` | `date` | Fecha capacitación — se elimina la hora |
| 6 | `FirstName` | `varchar(50)` | — | `nvarchar(255)` | Nombre |
| 7 | `LastName` | `varchar(50)` | — | `nvarchar(255)` | Apellido |
| 8 | `countrycode` | `varchar(255)` | — | `nvarchar(255)` | Código de país |
| 9 | `country` | `varchar(255)` | — | `nvarchar(300)` | País |
| 10 | `Estate` | `varchar(100)` | — | `nvarchar(1000)` | Estado / Provincia |
| 11 | `Ciudad` | `varchar(100)` | — | `nvarchar(300)` | Ciudad |
| 12 | `address` | `text` | — | `nvarchar(MAX)` | Dirección completa |
| 13 | `zip` | `varchar(16-20)` | — | `nvarchar(20)` | Código postal — ampliado 2026-08-20 |
| 14 | `Corpcode` | `varchar(12)` | — | `nvarchar(22)` | Código corporativo |
| 15 | `Corp` | `varchar(30-65)` | — | `nvarchar(100)` | Nombre corporativo — ampliado 2026-08-20 |
| 16 | `ingreso` | `int(1)` NOT NULL | — | `decimal(18,0)` | Fecha ingreso numérica |
| 17 | `egreso` | `int(1)` NOT NULL | — | `int` | Fecha egreso numérica |
| 18 | `rank` | `int(1)` NOT NULL | — | `int` | Ranking del cliente |
| 19 | `EstatusN` | `decimal(5,3)` | — | `decimal(11,3)` | Estatus numérico — ampliado 2026-08-19 |
| 20 | `EstatusL` | `varchar(20-64)` | — | `nvarchar(64)` | Estatus literal — ampliado 2026-08-20 |
| — | `createdAt` | *(no viene del origen)* | `NOW()` en etl_base.py | `datetime` | Fecha de inserción ETL en Silver |
| — | `updatedAt` | *(no viene del origen)* | `NULL` en etl_base.py | `datetime2` | Pendiente v2 — detección de cambios |
| — | `deletedAt` | *(no viene del origen)* | `NULL` en etl_base.py | `nvarchar(255)` | Pendiente v2 — detección de borrados |

> ⚠️ **Nota:** Las columnas `createdAt`, `updatedAt` y `deletedAt` **no se seleccionan desde la vista origen**. Son agregadas automáticamente por `etl_base.py`. El SELECT solo trae 20 columnas; el INSERT escribe 23.

---

## 🥉 Bronze Layer — Phones

### Patrón de carga: TRUNCATE + INSERT (Full Refresh)

| # | Vista Origen (MariaDB) | Tabla Destino | Servidor |
|---|---|---|---|
| 1 | `db_general.vwpersonalinfofb` | `source.Phonefb` | 242 |
| 2 | `db_general.vwpersonalinfobb` | `source.Phonebb` | 242 |
| 3 | `db_general.vwpersonalinfoml` | `source.Phoneml` | 242 |
| 4 | `db_general.vwpersonalinfofi` | `source.Phonefi` | 240 |
| 5 | `db_general.vwpersonalinfovc` | `source.Phonevc` | 240 |

### Estructura de columnas (común a todas las tablas Phone)

| Columna | Tipo | Descripción |
|---|---|---|
| `idphone` | INT AUTO | PK autonumérico |
| `clientid` | INT | FK → source.clients |
| `phone` | VARCHAR(30) | Número de teléfono |
| `atInsert` | SMALLDATETIME | Fecha de inserción (ETL) |
| `atUpdate` | SMALLDATETIME | NULL — pendiente v2 |

> ⚠️ **Razón del Full Refresh:** La data de teléfonos tiene errores tipográficos en origen. El TRUNCATE + INSERT garantiza que las correcciones se reflejen en Bronze.

---

## 🥇 Gold Layer

### Tablas finales procesadas por SPs SQL Server

| Tabla | SP que la llena | Registros |
|---|---|---|
| `gral.factClientes` | `sp_upsert_factClientes` | 283,523 |
| `gral.factClientesDetalle` | `sp_upsert_clients*_factClientesDetalle` | 484,181 |
| `gral.factPersonalInfo` | `sp_insert_phones_factPersonalInfo` | 452,665 |

---

## 🗄️ Tabla de Auditoría ETL

### `flybackDW.etl_audit_log` (MariaDB 192.168.10.242)

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | INT AUTO | PK |
| `paquete` | VARCHAR(100) | Nombre del DAG |
| `vista_origen` | VARCHAR(100) | Vista MariaDB origen |
| `tabla_destino` | VARCHAR(100) | Tabla SQL Server destino |
| `max_id_inicio` | BIGINT | MAX clientid antes de carga (0 para phones) |
| `filas_insertadas` | INT | Filas insertadas en el run |
| `tipo_ejecucion` | VARCHAR(20) | `SCHEDULED` o `MANUAL` |
| `estado` | VARCHAR(20) | `SUCCESS` o `ERROR` |
| `mensaje_error` | TEXT | Detalle del error si aplica |
| `fecha_inicio` | DATETIME | Inicio de la ejecución |
| `fecha_fin` | DATETIME | Fin de la ejecución |

---

*Última actualización: 2026-08-21*
