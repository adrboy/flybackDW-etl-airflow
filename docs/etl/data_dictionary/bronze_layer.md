# 📖 Diccionario de Datos — Bronze & Gold Layer

**Proyecto:** flybackDW ETL Pipeline  
**Base de datos destino:** `DBGeneralDW` (SQL Server 192.168.10.244)  
**Autor:** Andrés  
**Fecha:** Mayo 2026

---

## 📋 Modelo de Capas

```
Bronze Clients → Bronze Phones → Gold
(Incremental)   (Full Refresh)  (SPs SQL Server)
```

---

## 🥉 Bronze Layer — Clients

### Patrón de carga: Incremental por `clientid`

| # | Vista Origen (MariaDB) | Tabla Destino | Servidor | Columnas |
|---|---|---|---|---|
| 1 | `db_general.viewclientsfb` | `source.clientsfb` | 242 | 23 |
| 2 | `db_general.viewclientsbb` | `source.clientsbb` | 242 | 23 |
| 3 | `db_general.viewclientsml` | `source.clientsml` | 242 | 23 |
| 4 | `db_general.viewclientsfi` | `source.clientsfi` | 240 | 23 |
| 5 | `db_general.viewclientsvc` | `source.clientsvc` | 240 | 23 |

### Estructura de columnas (común a todas las tablas)

| Columna | Tipo | Descripción |
|---|---|---|
| `productid` | INT | ID del producto |
| `contractid` | INT | ID del contrato |
| `clientid` | INT | **PK incremental** — clave de carga |
| `email` | VARCHAR | Email del cliente |
| `capdata` | VARCHAR | Datos de capacidad |
| `FirstName` | VARCHAR | Nombre |
| `LastName` | VARCHAR | Apellido |
| `countrycode` | VARCHAR | Código de país |
| `country` | VARCHAR | País |
| `Estate` | VARCHAR | Estado/Provincia |
| `ciudad` | VARCHAR | Ciudad |
| `address` | VARCHAR | Dirección |
| `zip` | VARCHAR | Código postal |
| `corpcode` | VARCHAR | Código corporativo |
| `corp` | VARCHAR | Nombre corporativo |
| `ingreso` | DATETIME | Fecha de ingreso |
| `egreso` | DATETIME | Fecha de egreso |
| `rank` | INT | Ranking |
| `EstatusN` | INT | Estatus numérico |
| `EstatusL` | VARCHAR | Estatus literal |
| `createdAt` | DATETIME | **Fecha de inserción en Bronze** (ETL) |
| `updatedAt` | DATETIME | NULL — pendiente v2 |
| `deletedAt` | DATETIME | NULL — pendiente v2 |

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

> ⚠️ **Razón del Full Refresh:** La data de teléfonos tiene errores tipográficos en origen. El TRUNCATE + INSERT garantiza que las correcciones se reflejen en Bronze. En `etl_basephone_v2.py` se implementará lógica incremental con UPDATE.

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

*Última actualización: 28/05/2026*
