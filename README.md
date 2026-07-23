# 📚 Documentación del Proyecto ETL Bronze → Gold
**Proyecto:** Migración ETL SSIS → Apache Airflow  
**Autor:** Andrés  
**Fecha inicio:** Mayo 2026  
**Estado:** ✅ COMPLETADO

---

## 🎯 Resumen del Proyecto

Migración exitosa de 11 pipelines ETL desde **Microsoft SSIS** hacia **Apache Airflow** (Docker), conectando **MariaDB 242/240** como origen y **SQL Server 244** como destino, con arquitectura de 3 capas (Bronze Clients → Bronze Phones → Gold).

---

## 🗂️ Estructura de Documentación

| Carpeta | Contenido |
|---|---|
| `adr/` | Architecture Decision Records — decisiones técnicas |
| `runbook/` | Guías operativas y solución de problemas |
| `data_dictionary/` | Diccionario de datos de todas las capas |
| `user_stories/` | Historias de usuario en metodología Scrum |

---

## 🔗 Documentos Principales

- [ADR001 — Airflow vs SSIS](docs/etl/adr/ADR001_airflow_vs_ssis.md)
- [Runbook ETL](docs/etl/runbook/etl_bronze_runbook.md)
- [Diccionario de Datos](docs/etl/data_dictionary/bronze_layer.md)
- [US001 — ETL Pipeline](docs/etl/US001_bronze_layer.md)

---

## 📜 Arquitectura Inicial (As-Is) — Microsoft SSIS

Antes de la migración, los pipelines ETL operaban en **Microsoft SSIS** para la globalización de clientes de todos los productos y empresas del grupo. Los paquetes consolidaban datos de múltiples servidores MariaDB hacia SQL Server para alimentar el Data Warehouse corporativo.

```
pkgETL_MAESTRO_DW.dtsx  ← Orquestador principal
    │
    ├──► pkgClients42.dtsx   ← Clientes servidor 242
    │         ├── Flyback MAX    → Insert Flyback
    │         ├── BuyBack MAX    → Insert BuyBack
    │         └── MasterLink MAX → Insert MasterLink
    │
    ├──► pkgClients40.dtsx   ← Clientes servidor 240
    │         ├── Vacation C MAX → Insert Vacation C
    │         └── Financiamiento MAX → Insert Financiamiento
    │
    └──► pkgPhone.dtsx       ← Teléfonos todos los productos
              ├── Limpieza + Insert Phone FB
              ├── Limpieza + Insert Phone BuyBack
              ├── Limpieza + Insert Phone MasterLink
              ├── Limpieza + Insert Phone Financiamiento
              └── Limpieza + Insert Phone Vacation Center
```

**Limitaciones identificadas:**
- Sin reintentos automáticos ante fallos de conexión
- Dependencia de licencias SQL Server Integration Services
- Difícil monitoreo sin acceso al servidor Windows
- No permitía ejecución individual de un pipeline fallido

---

## 🏗️ Arquitectura Final (To-Be) — Apache Airflow

```
DAG Master DW (futuro)
    │
    ├──► dag_masterclients   ← Bronze Clients (incremental)
    │         ├── dag_clientsfi_240
    │         ├── dag_clientsvc_240
    │         ├── dag_clientsfb_242
    │         ├── dag_clientsbb_242
    │         └── dag_clientsml_242
    │
    ├──► dag_masterphones    ← Bronze Phones (TRUNCATE+INSERT)
    │         ├── dag_phonefi_240
    │         ├── dag_phonevc_240
    │         ├── dag_phonefb_242
    │         ├── dag_phonebb_242
    │         └── dag_phoneml_242
    │
    └──► dag_master_gold     ← Gold Layer (SPs SQL Server)
              ├── sp_etl_maestro
              └── sp_insert_phones_factPersonalInfo
```

**Mejoras implementadas:**
- ✅ Auditoría triple (Airflow + SQL Server + archivos .txt)
- ✅ Reintentos automáticos (3 intentos, 1 minuto de pausa)
- ✅ DAGs individuales ejecutables ante fallos parciales
- ✅ Monitoreo desde UI web sin acceso al servidor
- ✅ Código Python versionable en Git
- ✅ Open Source — sin licencias adicionales

---

## 📊 Resultados Finales

| Tabla Gold | Registros |
|---|---|
| `gral.factClientes` | 283,523 |
| `gral.factClientesDetalle` | 484,181 |
| `gral.factPersonalInfo` | 452,665 |

---

*Última actualización: 28/05/2026*
