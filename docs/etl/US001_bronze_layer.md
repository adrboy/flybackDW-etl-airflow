# 📝 US001 — ETL Pipeline Bronze → Gold

**Proyecto:** flybackDW ETL Pipeline  
**Sprint:** 1-2  
**Estado:** ✅ COMPLETADO  
**Autor:** Andrés  
**Fecha:** Mayo 2026

---

## 👤 Historia de Usuario

> **Como** Ingeniero de Datos  
> **Quiero** automatizar la carga completa de datos desde MariaDB hacia SQL Server con capas Bronze y Gold  
> **Para** eliminar la dependencia de SSIS, tener auditoría completa y poder monitorear las ejecuciones desde cualquier lugar

---

## ✅ Criterios de Aceptación

| # | Criterio | Estado |
|---|---|---|
| 1 | 5 vistas de Clients migradas (incremental) | ✅ |
| 2 | 5 vistas de Phones migradas (full refresh) | ✅ |
| 3 | Carga incremental por `clientid` para Clients | ✅ |
| 4 | TRUNCATE + INSERT para Phones | ✅ |
| 5 | Auditoría en tabla MariaDB por ejecución | ✅ |
| 6 | Archivo `.txt` de log por ejecución | ✅ |
| 7 | Reintentos automáticos (3 intentos, 1 min) | ✅ |
| 8 | DAGs individuales ejecutables ante fallo | ✅ |
| 9 | DAG master Clients orquestador | ✅ |
| 10 | DAG master Phones orquestador | ✅ |
| 11 | DAG master Gold con SPs SQL Server | ✅ |
| 12 | Documentación técnica completa | ✅ |

---

## 📋 Tareas Técnicas

### Sprint 1 ✅
- [x] Configurar conexiones en Airflow (MariaDB, MariaDB240, MSSQL244)
- [x] Crear estructura de carpetas del proyecto
- [x] Desarrollar `audit_logger.py`
- [x] Desarrollar `etl_base.py` (incremental)
- [x] Desarrollar `db_connections.py`
- [x] Desarrollar 5 DAGs hijos de Clients
- [x] Desarrollar `dag_masterclients.py`
- [x] Validar runs exitosos con datos reales

### Sprint 2 ✅
- [x] Desarrollar `etl_basephone.py` (TRUNCATE+INSERT)
- [x] Desarrollar 5 DAGs hijos de Phones
- [x] Desarrollar `dag_masterphones.py`
- [x] Desarrollar `dag_master_gold.py`
- [x] Validar capa Gold con SPs SQL Server
- [x] Documentación completa (ADR, Runbook, Data Dictionary)

---

## 🏗️ Arquitectura Implementada

```
MariaDB 242/240 (Bronze)
    ↓ incremental clientid
source.clients* (SQL Server)
    ↓ TRUNCATE+INSERT
source.Phone* (SQL Server)
    ↓ SPs UPSERT
gral.factClientes / factClientesDetalle / factPersonalInfo
```

---

## 📈 Métricas Finales

| Métrica | Valor |
|---|---|
| DAGs completados | 13 ✅ |
| Tablas Bronze Clients | 5 ✅ |
| Tablas Bronze Phones | 5 ✅ |
| Tablas Gold | 3 ✅ |
| Total filas Bronze Clients | ~670,000+ |
| Total filas Bronze Phones | ~670,060 |
| Total filas Gold | ~1,220,000+ |
| Auditoría | Triple (Airflow + SQL Server + .txt) |

---

## 🔑 Lecciones Aprendidas

| # | Lección |
|---|---|
| 1 | Nunca hardcodear credenciales — usar Hook de Airflow |
| 2 | `try/except/finally` — log siempre se ejecuta |
| 3 | `fetchmany(1000)` — balance velocidad/memoria |
| 4 | Ctrl+S siempre — punto blanco = sin guardar |
| 5 | `wc -l` para verificar sincronización en Docker |
| 6 | Separar responsabilidades: `etl_base` vs `etl_basephone` |
| 7 | ExternalTaskSensor requiere `execution_delta` para producción |
| 8 | Bronze es espejo fiel — transformaciones van en Gold |

---

## 🔮 Próximas Versiones

| Tarea | Versión |
|---|---|
| `etl_basephone_v2.py` — incremental con UPDATE | v2 |
| Corregir `ExternalTaskSensor` con `timedelta` | v1.1 |
| Mover credenciales a `.env` | v1.1 |
| Publicar en GitHub | v1.1 |
| `master_bronze_DW.py` — orquestador global | v2 |

---

## ✅ Definition of Done

1. ✅ DAG corre sin errores en Airflow
2. ✅ Datos correctos en SQL Server
3. ✅ `etl_audit_log` registra la ejecución
4. ✅ Archivo `.txt` de log generado
5. ✅ Código guardado y sincronizado en Docker
6. ✅ Documentación actualizada

---

*Documento completado el 28/05/2026*
