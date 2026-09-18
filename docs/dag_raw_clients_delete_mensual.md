# 🗑️ DAG: dag_raw_clients_delete_mensual
**Fecha:** 18 de Septiembre 2026  
**Proyecto:** flybackDW — RAW Layer v2  
**Autor:** Andrés

---

## 🎯 Objetivo

Ejecutar mensualmente los 5 Stored Procedures de soft delete
que marcan como eliminados los registros huérfanos en las tablas RAW —
es decir, clientid que ya no existe en el sistema origen.

---

## 🏗️ Arquitectura — 3 Capas

```
dag_raw_clients_delete_mensual.py  ← DAG orquestador
        │
        ├── etl_raw_delete_ne.py   ← Capa Negocio
        │     orquestar('242')         try/except, email, log .txt, return True/False
        │     orquestar('240')
        │
        └── etl_raw_delete_da.py   ← Capa Datos (common/)
              get_conexion()           devuelve conn_id de Airflow
              get_procedimientos()     devuelve lista de SPs DELETE
              get_log_config()         devuelve tabla y ruta de log
              ejecutar_sp()            CALL sp() — lanza excepción si falla
```

> 💡 Misma arquitectura de 3 capas que el CDC diario.
> Separado por ser un proceso de naturaleza distinta — reconciliación,
> no captura diaria.

---

## ⏰ Schedule

| Campo | Valor |
|-------|-------|
| Cron | `0 3 3 * *` |
| Frecuencia | Mensual |
| Día | 3 de cada mes |
| Hora | 3:00am |
| Razón día 3 | Día 1 = cierre de mes con actividad intensa. Día 2 = buffer. Día 3 = seguro |
| Modo | PARALELO — instancia 242 y 240 al mismo tiempo |
| Reintentos | 1 por task, 5 min entre intentos |

---

## 🗂️ Archivos del DAG

| Archivo | Carpeta | Capa | Descripción |
|---------|---------|------|-------------|
| `dag_raw_clients_delete_mensual.py` | `etl/` | DAG | Orquestador, schedule, tasks paralelas |
| `etl_raw_delete_ne.py` | `etl/` | Negocio | Orquesta SPs, maneja errores, email, log .txt |
| `etl_raw_delete_da.py` | `common/` | Datos | Conexiones, lista SPs DELETE, ejecutar CALL |

---

## 📦 Stored Procedures DELETE

### Instancia 242

| SP | Origen | RAW afectada | PK |
|----|--------|--------------|----|
| `etl_clientsfb_delete` | `customers.fb_clients` | `tbl_raw_clientsfb` | `clientid` |
| `etl_clientsbb_delete` | `buyback.clients` | `tbl_raw_clientsbb` | `clientid` |
| `etl_clientsml_delete` | `masterlinks.clients` | `tbl_raw_clientsml` | `clientid` |

### Instancia 240

| SP | Origen | RAW afectada | PK |
|----|--------|--------------|----|
| `etl_clientsfi_delete` | `financiamiento.clients + credits` | `tbl_raw_clientsfi` | `client_id` |
| `etl_clientsvc_delete` | `vtw.catowners + p_data` | `tbl_raw_clientsvc` | `tradedid` |

---

## 🔄 Lógica Soft Delete

```sql
-- Patrón de cada SP DELETE:
UPDATE tbl_raw_clientsXX  raw
SET    raw.deletedAt = NOW()
WHERE  raw.deletedAt IS NULL          -- no borrado previamente
  AND  NOT EXISTS (
           SELECT 1
           FROM   [tabla_origen]  src
           WHERE  src.[pk] = raw.[pk]
             AND  src.contractid IS NOT NULL
       );
```

**Soft delete** — no borra el registro, solo marca `deletedAt = NOW()`:

```
deletedAt IS NULL   → cliente activo en origen ✅
deletedAt = fecha   → cliente ya no existe en origen ⚠️
```

---

## 📋 Log de Auditoría

Cada ejecución genera **1 registro por SP** en `db_general.etl_audit_log`:

| tipo_ejecucion | Descripción |
|----------------|-------------|
| `VERIFY-DELETE` | Huérfanos marcados con deletedAt |

```
filas_insertadas = 0  → normal, nadie fue borrado ese mes
filas_insertadas = N  → N registros marcados como eliminados — revisar
```

```sql
-- Consulta de verificación post-ejecución
SELECT *
FROM   db_general.etl_audit_log
WHERE  tipo_ejecucion = 'VERIFY-DELETE'
ORDER BY id DESC
LIMIT 10;

-- Ver registros marcados como eliminados
SELECT *
FROM   db_general.tbl_raw_clientsfb
WHERE  deletedAt IS NOT NULL
ORDER BY deletedAt DESC;
```

---

## 🔑 Decisiones de Diseño

| # | Decisión | Razón |
|---|----------|-------|
| 1 | DAG separado del CDC diario | Delete es reconciliación, no captura. Distintos ciclos de vida |
| 2 | Mensual en lugar de diario | Borrados de clientes son eventos raros. Auditoría confirmó 0 huérfanos en primer run |
| 3 | Día 3 del mes a las 3am | Día 1 = cierre de mes activo. Día 2 = buffer. Día 3 = ventana segura nocturna |
| 4 | Soft delete (deletedAt) en lugar de DELETE físico | Trazabilidad forense. Silver decide si excluir o no |
| 5 | SP propio por fuente | Cada origen tiene PK y lógica distintos (clientid / client_id / tradedid) |
| 6 | 1 reintento con 5 min de espera | Delete mensual no es urgente — espera más que el CDC diario |

---

## 🚀 Primer Run Manual Exitoso

```
SP    : etl_clientsfb_delete
Fecha : 2026-09-18 11:24
Estado: ✅ OK
Filas : 0 huérfanos — RAW sincronizada correctamente
max_id: 384,440
```

---

## ⚠️ Cuándo Preocuparse

```
filas_insertadas > 0   → revisar qué clientid fueron borrados en origen
                          ¿borrado legítimo o error de datos?
                          consultar con el equipo de negocio

filas_insertadas > 100 → alerta — posible problema en origen
                          no ejecutar Silver hasta investigar

estado = ERROR         → revisar mensaje_error en etl_audit_log
                          posible problema de conexión o permisos
```

---

*Documento generado el 18/09/2026*
