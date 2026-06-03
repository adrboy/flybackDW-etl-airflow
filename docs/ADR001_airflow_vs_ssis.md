# ADR001 — Migración de SSIS a Apache Airflow

**Estado:** ✅ Aprobado  
**Fecha:** Mayo 2026  
**Autor:** Andrés  

---

## 📋 Contexto

La empresa opera pipelines ETL construidos en **Microsoft SSIS** que cargan datos desde servidores MariaDB (242 y 240) hacia SQL Server (244) en una capa Bronze. El proceso actual presenta los siguientes problemas:

- Sin auditoría de filas insertadas por ejecución
- Sin reintentos automáticos ante fallos de conexión
- Difícil de monitorear sin acceso al servidor Windows donde corre SSIS
- No permite ejecución individual de un pipeline fallido sin correr todo el paquete
- Dependencia de licencias de SQL Server Integration Services

---

## 🤔 Decisión

Se decide migrar los pipelines ETL de **SSIS a Apache Airflow** corriendo en **Docker** sobre la misma infraestructura existente.

---

## ✅ Razones de la decisión

| Factor | SSIS | Apache Airflow |
|---|---|---|
| **Costo** | Licencia SQL Server | Open Source |
| **Monitoreo** | Solo en servidor Windows | UI Web accesible desde cualquier lugar |
| **Auditoría** | Manual o con jobs adicionales | Integrada en el DAG |
| **Reintentos** | Configuración compleja | `retries=3, retry_delay=60` nativo |
| **Ejecución individual** | Requiere abrir SSIS | Trigger manual por DAG |
| **Versionamiento** | Archivos `.dtsx` binarios | Código Python en Git |
| **Escalabilidad** | Limitada al servidor | Docker + múltiples workers |
| **Portabilidad** | Solo Windows | Linux/Docker/Cloud |

---

## 🏗️ Arquitectura implementada

```
MariaDB 242 (origen)     SQL Server 244 (destino)
viewclientsfb     ──►    source.clientsfb
viewclientsbb     ──►    source.clientsbb
viewclientsml     ──►    source.clientsml

MariaDB 240 (origen)
viewclientsfi     ──►    source.clientsfi
viewclientsvc     ──►    source.clientsvc
```

### Patrón de diseño aplicado
Equivalente al **Repository Pattern** de C#/.NET:
- `common/etl_base.py` → Capa de Datos
- `common/audit_logger.py` → Capa de Datos
- `common/db_connections.py` → Configuración
- `etl/dag_*.py` → Capa de Negocio

---

## ⚠️ Consecuencias

### Positivas
- Auditoría completa: tabla MariaDB + archivo `.txt` por ejecución
- Reintentos automáticos (3 intentos, pausa 1 minuto)
- DAGs individuales ejecutables ante fallos parciales
- Código Python versionable en Git
- Monitoreo desde UI web sin acceso al servidor

### Negativas / Riesgos
- Requiere mantenimiento del contenedor Docker
- Curva de aprendizaje de Python para el equipo
- Dependencia de la red interna para conectar los servidores

---

## 🔄 Alternativas descartadas

| Alternativa | Razón de descarte |
|---|---|
| Mantener SSIS | Sin auditoría, sin reintentos, costo de licencia |
| Azure Data Factory | Costo mensual, requiere conexión a Azure |
| Python scripts + cron | Sin UI de monitoreo, sin reintentos nativos |

---

*Documento creado el 26/05/2026*
