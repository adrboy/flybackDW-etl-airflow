# 🔄 Migración ETL: SSIS → Airflow
**Fecha:** 26 de Mayo 2026  
**Proyecto:** flybackDW — Bronze Layer  
**Autor:** Andrés

---

## 🎯 Objetivo del Día

Construir el primer DAG ETL completo que migra datos de **MariaDB 242** hacia **SQL Server 244** (capa Bronze), con auditoría doble (tabla + archivo `.txt`), arquitectura profesional y separación de responsabilidades.

---

## ✅ Logros del Día

### Paso 2 — Estructura de carpetas
```
DockersETL/
└── dags/
    ├── common/
    │   ├── __init__.py
    │   ├── audit_logger.py
    │   ├── db_connections.py
    │   └── etl_base.py
    └── etl/
        ├── __init__.py
        └── dag_clientsfb_242.py
```

### Paso 3 — `audit_logger.py`
Módulo de auditoría con dos funciones:
- `registrar_log()` → escribe en tabla `flybackDW.etl_audit_log`
- `escribir_log_txt()` → genera archivo `etl_<vista>_FB_log_YYYYMMDDHHMMSS.txt`

### Paso 4 — `etl_base.py`
Módulo genérico reutilizable con dos funciones:
- `get_max_id()` → obtiene `MAX(clientid)` de SQL Server destino
- `ejecutar_insert()` → lee MariaDB en batches de 1,000 e inserta en SQL Server

### Paso 5 — `dag_clientsfb_242.py`
Primer DAG hijo completo y funcional.

---

## 🏗️ Arquitectura Final

```
DAG Master (próximo)
    │
    ├──► DAG_240 (viewclientsfi, viewclientsvc)
    │         ↓ cuando termina
    └──► DAG_242 (viewclientsfb, viewclientsbb, viewclientsml)
```

### Filosofía de separación
| Capa | Archivo | Responsabilidad |
|---|---|---|
| Datos | `etl_base.py` | Lógica de acceso a BD |
| Datos | `audit_logger.py` | Escritura de logs |
| Configuración | `db_connections.py` | Credenciales centralizadas |
| Negocio | `dag_clientsfb_242.py` | Orquesta el flujo ETL |

> 💡 Mismo patrón que **Repository Pattern** en VB.NET / C# con Entity Framework

---

## 📦 Inventario de Vistas y Tablas

| # | Vista Origen (MariaDB) | Tabla Destino (SQL Server) | Servidor |
|---|---|---|---|
| 1 | `db_general.viewclientsfb` | `source.clientsfb` | 242 ✅ |
| 2 | `db_general.viewclientsbb` | `source.clientsbb` | 242 |
| 3 | `db_general.viewclientsml` | `source.clientsml` | 242 |
| 4 | `db_general.viewclientsfi` | `source.clientsfi` | 240 |
| 5 | `db_general.viewclientsvc` | `source.clientsvc` | 240 |

---

## 🚀 Primer Run Exitoso

```
DAG:        dag_clientsfb_242
Fecha:      2026-05-26 20:40:15
Estado:     ✅ SUCCESS
Duración:   3 segundos
```

### Resultado verificado

| | COUNT | MAX clientid |
|---|---|---|
| **Antes** | 367,502 | 368,210 |
| **Después** | 370,223 | 370,931 |
| **Insertadas** | **+2,721 filas** | ✅ |

---

## 🔑 Lecciones Aprendidas

| # | Lección |
|---|---|
| 1 | **Nunca hardcodear credenciales** — siempre usar Hook de Airflow |
| 2 | **Ctrl+S siempre** — punto blanco en pestaña VSCode = sin guardar |
| 3 | **wc -l** para verificar sincronización en Docker |
| 4 | **Hook de Airflow** usa conexiones configuradas en la UI |
| 5 | **`try/except/else`** — else solo corre si no hubo errores |
| 6 | **`fetchmany(1000)`** — balance perfecto velocidad/memoria |

---

## 📋 Próximos Pasos

- [ ] `dag_clientsbb_242.py`
- [ ] `dag_clientsml_242.py`
- [ ] `master_bronze_242.py`
- [ ] Conexión MariaDB 240 + DAGs del 240
- [ ] `master_bronze_DW.py`

---

*Documento generado el 26/05/2026*
