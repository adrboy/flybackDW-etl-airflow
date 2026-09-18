# 🔄 DAG: dag_raw_clients_cdc_diario
**Fecha:** 17-18 de Septiembre 2026  
**Proyecto:** flybackDW — RAW Layer v2  
**Autor:** Andrés

---

## 🎯 Objetivo

Ejecutar diariamente los 5 Stored Procedures CDC (Change Data Capture)
que sincronizan las tablas RAW de clientes en MariaDB.
Reemplaza el flujo anterior que leía directamente de las vistas hacia SQL Server.

---

## 🏗️ Arquitectura — 3 Capas

```
dag_raw_clients_cdc_diario.py   ← DAG orquestador (capa presentación/negocio)
        │
        ├── etl_raw_ne.py       ← Capa Negocio
        │     orquestar('242')      try/except, email, log .txt, return True/False
        │     orquestar('240')
        │
        └── etl_raw_da.py       ← Capa Datos (common/)
              get_conexion()        devuelve conn_id de Airflow
              get_procedimientos()  devuelve lista de SPs
              get_log_config()      devuelve tabla y ruta de log
              ejecutar_sp()         CALL sp() — lanza excepción si falla
              registrar_log()       INSERT en etl_audit_log
```

> 💡 Mismo patrón que capas DA/NE/UI en VB.NET — sin capa de presentación,
> el DAG actúa como ejecutor directo de la capa negocio.

---

## ⏰ Schedule

| Campo | Valor |
|-------|-------|
| Cron | `0 8,18 * * *` |
| Frecuencia | 2 veces al día |
| Hora 1 | 8:00am — antes de que inicien transacciones (9am) |
| Hora 2 | 6:00pm — al cierre del día laboral |
| Modo | PARALELO — instancia 242 y 240 al mismo tiempo |
| Reintentos | 2 por task, 60s entre intentos |

---

## 🗂️ Archivos del DAG

| Archivo | Carpeta | Capa | Descripción |
|---------|---------|------|-------------|
| `dag_raw_clients_cdc_diario.py` | `etl/` | DAG | Orquestador, schedule, tasks paralelas |
| `etl_raw_ne.py` | `etl/` | Negocio | Orquesta SPs, maneja errores, email, log .txt |
| `etl_raw_da.py` | `common/` | Datos | Conexiones, lista SPs, ejecutar CALL, audit log |
| `insert_audit_log_raw.sql` | `sql/clients/` | SQL | INSERT en etl_audit_log |

---

## 📦 Stored Procedures CDC

### Instancia 242

| SP | Origen | Destino RAW | Registros carga inicial |
|----|--------|-------------|------------------------|
| `etl_clientsfb` | `customers.fb_clients` | `tbl_raw_clientsfb` | 383,545 |
| `etl_clientsbb` | `buyback.clients` | `tbl_raw_clientsbb` | 7,974 |
| `etl_clientsml` | `masterlinks.clients` | `tbl_raw_clientsml` | 791 |

### Instancia 240

| SP | Origen | Destino RAW | Registros carga inicial |
|----|--------|-------------|------------------------|
| `etl_clientsfi` | `financiamiento.clients + credits + clients_email` | `tbl_raw_clientsfi` | 61,773 |
| `etl_clientsvc` | `vtw.catowners + p_data` | `tbl_raw_clientsvc` | 58,358 |

**Total carga inicial: 512,441 registros**

---

## 🔄 Lógica CDC por SP

Cada SP ejecuta dos bloques:

```
BLOQUE 1 — INSERT nuevos
  WHERE clientid > MAX(clientid en RAW)
  → Solo registros nuevos desde la última ejecución

BLOQUE 2 — UPDATE modificados
  WHERE columnas difieren (comparación directa)
  OR    raw.updatedAt < src.updatedAt (red de seguridad)
  → COALESCE en cada columna para evitar falsos positivos con NULL
```

---

## 📋 Log de Auditoría

Cada ejecución genera **2 registros por SP** en `db_general.etl_audit_log`:

| tipo_ejecucion | Descripción |
|----------------|-------------|
| `DIARIO-INSERT` | Filas nuevas insertadas |
| `DIARIO-UPDATE` | Filas modificadas actualizadas |

```sql
-- Consulta de verificación
SELECT *
FROM   db_general.etl_audit_log
WHERE  paquete IN ('etl_clientsfb','etl_clientsbb','etl_clientsml')
ORDER BY id DESC
LIMIT 10;
```

---

## 🔑 Decisiones de Diseño

| # | Decisión | Razón |
|---|----------|-------|
| 1 | RAW forense — nombres idénticos al origen | Trazabilidad, no transformar en Bronze |
| 2 | Watermark por autonumérico (clientid) | Más confiable que updatedAt del origen |
| 3 | updatedAt origen como red de seguridad | Plan B cuando columnas no detectan cambio |
| 4 | Log granular INSERT/UPDATE por separado | Auditoría clara — saber exactamente qué pasó |
| 5 | Paralelo 242 y 240 | Instancias independientes, sin competencia de recursos |
| 6 | 8am y 6pm | Ventana limpia — transacciones inician a las 9am |
| 7 | Email por instancia | Granularidad en notificaciones |
| 8 | @noreply.flyback para emails vacíos | Unicidad en Gold sin perder registros |

---

## 🚀 Primer Run Exitoso

```
DAG   : dag_raw_clients_cdc_diario
Fecha : 2026-09-18 11:01
Estado: ✅ SUCCESS
Tasks : cdc_instancia_242 ✅ | cdc_instancia_240 ✅
Emails: 4 notificaciones recibidas ✅
```

---

## 📋 DAGs Desactivados (pendiente Silver)

Los siguientes DAGs fueron desactivados (`schedule_interval = None`)
y serán reutilizados para la capa Silver quincenal RAW → SQL Server:

| DAG | Instancia | Estado |
|-----|-----------|--------|
| `dag_masterclients` | 242+240 | Desactivado ⏸ |
| `dag_clientsfb_242` | 242 | Desactivado ⏸ |
| `dag_clientsbb_242` | 242 | Desactivado ⏸ |
| `dag_clientsml_242` | 242 | Desactivado ⏸ |
| `dag_clientsfi_240` | 240 | Desactivado ⏸ |
| `dag_clientsvc_240` | 240 | Desactivado ⏸ |

---

*Documento generado el 18/09/2026*
