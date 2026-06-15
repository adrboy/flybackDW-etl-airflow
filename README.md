# 🔄 ETL Pipeline: SSIS → Apache Airflow
> Migración de pipelines ETL desde Microsoft SSIS hacia Apache Airflow en Docker

![Python](https://img.shields.io/badge/Python-3.12-blue)
![Airflow](https://img.shields.io/badge/Airflow-2.9.3-green)
![Docker](https://img.shields.io/badge/Docker-Compose-blue)
![Status](https://img.shields.io/badge/Status-En%20Progreso-yellow)

---

## 📋 Descripción

Pipeline ETL profesional que migra datos de clientes y teléfonos desde **MariaDB** (servidores 242 y 240) hacia **SQL Server** (servidor 244) con arquitectura de 3 capas:

```
Bronze Clients (incremental) → Bronze Phones (full refresh) → Gold (SPs SQL Server)
```

---

## 🏗️ Arquitectura

```
dag_masterclients   ← Bronze Clients (5 vistas, incremental por clientid)
dag_masterphones    ← Bronze Phones (5 vistas, TRUNCATE+INSERT)
dag_master_gold     ← Gold Layer (2 Stored Procedures SQL Server)
```

---

## 🛠️ Stack Tecnológico

| Tecnología | Uso |
|---|---|
| Apache Airflow 2.9.3 | Orquestación de pipelines |
| Docker / Docker Compose | Containerización |
| Python 3.12 | Lógica ETL |
| MariaDB | Base de datos origen |
| SQL Server | Base de datos destino |
| pymssql / MySqlHook | Conectores de base de datos |

---

## 🚀 Instalación

### Prerrequisitos
- Docker Desktop instalado
- Git instalado

### Pasos

```bash
# 1. Clonar el repositorio
git clone https://github.com/tu-usuario/flybackDW-etl-airflow.git
cd flybackDW-etl-airflow

# 2. Crear archivo de variables de entorno
cp .env.example .env
# Editar .env con tus credenciales

# 3. Levantar los contenedores
docker-compose up -d

# 4. Abrir Airflow UI
# http://localhost:8085
# Usuario: airflow / Password: airflow
```

### Configurar conexiones en Airflow UI
Ir a **Admin → Connections** y crear:

| Conn Id | Tipo | Host | Puerto |
|---|---|---|---|
| `MariaDB` | MySQL | 192.168.10.242 | 3306 |
| `MariaDB240` | MySQL | 192.168.10.240 | 3306 |
| `MSSQL244` | Microsoft SQL Server | 192.168.10.244 | 1433 |

---

## 📁 Estructura del Proyecto

```
DockersETL/
├── .env.example          ← plantilla de variables de entorno
├── .gitignore
├── docker-compose.yml    ← configuración de contenedores
├── docs/                 ← documentación completa
│   ├── adr/              ← Architecture Decision Records
│   ├── data_dictionary/  ← diccionario de datos
│   ├── runbook/          ← guías operativas
│   └── user_stories/     ← historias de usuario (Scrum)
└── dags/
    ├── common/           ← módulos reutilizables
    │   ├── audit_logger.py
    │   ├── db_connections.py
    │   ├── email_notifier.py
    │   ├── etl_base.py
    │   └── etl_basephone.py
    └── etl/              ← DAGs del pipeline
        ├── dag_clients*  ← Bronze Clients
        ├── dag_phones*   ← Bronze Phones
        ├── dag_master*   ← Orquestadores
        └── dag_master_gold.py
```

---

## 📚 Documentación

| Documento | Descripción |
|---|---|
| [ADR001](docs/adr/ADR001_airflow_vs_ssis.md) | Por qué Airflow sobre SSIS |
| [Runbook](docs/runbook/etl_bronze_runbook.md) | Operación y troubleshooting |
| [Data Dictionary](docs/data_dictionary/bronze_layer.md) | Estructura de datos |
| [User Story](docs/user_stories/US001_bronze_layer.md) | Requerimientos y criterios |

---

## 🔔 Notificaciones ETL

El módulo `dags/common/email_notifier.py` envía notificaciones por correo al finalizar cada proceso ETL con el contenido del log adjunto.

---

## 👤 Autor

**Andrés** — Ingeniero de Datos  
Proyecto en curso — Junio 2026

---

*⚠️ Proyecto en desarrollo activo — Última actualización: 03/06/2026*
