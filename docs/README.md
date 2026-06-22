# Documentacion del Proyecto ETL Bronze -> Gold

Proyecto: Migracion ETL SSIS -> Apache Airflow  
Autor: Andres  
Fecha inicio: Mayo 2026  
Ultima actualizacion: 2026-06-22  
Estado: En operacion, con refactor batch en Bronze Clients

## Resumen

Este proyecto migra pipelines ETL desde Microsoft SSIS hacia Apache Airflow ejecutado en Docker. La solucion conecta origenes MariaDB 240/242 con SQL Server 244 y mantiene procesos complementarios sobre la base MariaDB `flybackDW`.

La arquitectura actual trabaja con tres capas principales:

- Bronze Clients: carga incremental desde vistas MariaDB hacia tablas `source.clients*` en SQL Server.
- Bronze Phones: carga completa por `TRUNCATE + INSERT` hacia tablas de telefonos.
- Gold: ejecucion de stored procedures en SQL Server para poblar tablas finales del Data Warehouse.

## Cambio reciente: refactor batch sin SQL embebido

La capa Bronze Clients fue refactorizada para reducir SQL embebido en Python y mejorar el rendimiento de insercion.

Cambios principales:

- `dags/common/etl_base.py` centraliza el motor ETL reutilizable.
- `get_max_id` ahora carga su consulta desde `dags/sql/clients/get_max_id.sql`.
- Los `SELECT` de origen y los `INSERT` de destino viven en archivos `.sql`.
- Los inserts usan lotes de 1,000 filas con `cursor_destino.executemany(...)`.
- El DAG solo define constantes de configuracion: vista origen, tabla destino y rutas SQL.
- El flujo conserva auditoria en tabla `flybackDW.etl_audit_log` y logs `.txt`.

Flujo actual de Bronze Clients:

```text
DAG cliente
  |
  v
get_max_id()
  |
  +-- carga sql/clients/get_max_id.sql
  +-- obtiene MAX(clientid) en SQL Server destino
  |
  v
ejecutar_insert()
  |
  +-- carga SELECT externo desde dags/sql/clients/select_*.sql
  +-- carga INSERT externo desde dags/sql/clients/insert_*.sql
  +-- lee MariaDB con fetchmany(BATCH_SIZE=1000)
  +-- agrega createdAt, updatedAt, deletedAt
  +-- inserta en SQL Server con executemany
  |
  v
registrar_log() + escribir_log_txt()
```

## Arquitectura actual

```text
Docker Compose
  |
  +-- postgres:13
  |     Metadata database de Airflow
  |
  +-- airflow-webserver
  |     UI en puerto 8085
  |
  +-- airflow-scheduler
        Ejecuta DAGs con LocalExecutor

Airflow
  |
  +-- dags/etl
  |     DAGs Bronze y Gold
  |
  +-- dags/common
  |     Helpers compartidos: conexiones, auditoria, SQL loader, motores ETL
  |
  +-- dags/sql
  |     SQL externo usado por los pipelines
  |
  +-- dags/etl_flyback
        Procesos operativos MariaDB flybackDW
```

## Estructura de documentacion

| Carpeta | Contenido |
|---|---|
| `adr/` | Architecture Decision Records y decisiones tecnicas |
| `runbook/` | Guias operativas y solucion de problemas |
| `data_dictionary/` | Diccionario de datos de capas Bronze/Gold |
| `user_stories/` | Historias de usuario |
| `tbInicioSolAutPag/` | Documentacion de procesos Flyback especificos |

Documentos principales:

- [ADR001 - Airflow vs SSIS](adr/ADR001_airflow_vs_ssis.md)
- [Runbook ETL](runbook/etl_bronze_runbook.md)
- [Diccionario de Datos](data_dictionary/bronze_layer.md)
- [US001 - ETL Pipeline](user_stories/US001_bronze_layer.md)
- [Arquitectura actual](arquitectura_actual.md)

## Inventario de DAGs principales

### Bronze Clients

| DAG | Origen | Destino | Patron |
|---|---|---|---|
| `dag_clientsfi_240` | `db_general.viewclientsfi` en MariaDB 240 | `source.clientsfi` | Incremental por `clientid` |
| `dag_clientsvc_240` | `db_general.viewclientsvc` en MariaDB 240 | `source.clientsvc` | Incremental por `clientid` |
| `dag_clientsfb_242` | `db_general.viewclientsfb` en MariaDB 242 | `source.clientsfb` | Incremental por `clientid` |
| `dag_clientsbb_242` | `db_general.viewclientsbb` en MariaDB 242 | `source.clientsbb` | Incremental por `clientid` |
| `dag_clientsml_242` | `db_general.viewclientsml` en MariaDB 242 | `source.clientsml` | Incremental por `clientid` |

Orquestador:

- `dag_masterclients`: ejecuta los DAGs de clientes en secuencia semanal.

### SQL externo de Bronze Clients

| Proceso | SELECT | INSERT |
|---|---|---|
| Max ID comun | `sql/clients/get_max_id.sql` | No aplica |
| Clients FI 240 | `sql/clients/select_clientsfi_240.sql` | `sql/clients/insert_clientsfi_240.sql` |
| Clients VC 240 | `sql/clients/select_clientsvc_240.sql` | `sql/clients/insert_clientsvc_240.sql` |
| Clients FB 242 | `sql/clients/select_clientsfb_242.sql` | `sql/clients/insert_clientsfb_242.sql` |
| Clients BB 242 | `sql/clients/select_clientsbb_242.sql` | `sql/clients/insert_clientsbb_242.sql` |
| Clients ML 242 | `sql/clients/select_clientsml_242.sql` | `sql/clients/insert_clientsml_242.sql` |

### Bronze Phones

| DAG | Origen | Destino | Patron |
|---|---|---|---|
| `dag_phonefi_240` | MariaDB 240 | SQL Server 244 | `TRUNCATE + INSERT` |
| `dag_phonevc_240` | MariaDB 240 | SQL Server 244 | `TRUNCATE + INSERT` |
| `dag_phonefb_242` | MariaDB 242 | SQL Server 244 | `TRUNCATE + INSERT` |
| `dag_phonebb_242` | MariaDB 242 | SQL Server 244 | `TRUNCATE + INSERT` |
| `dag_phoneml_242` | MariaDB 242 | SQL Server 244 | `TRUNCATE + INSERT` |

Orquestador:

- `dag_masterphones`: ejecuta los DAGs de telefonos en secuencia semanal.

### Gold

| DAG | Tarea | Destino |
|---|---|---|
| `dag_master_gold` | `EXEC [dw_etl].[sp_etl_maestro]` | SQL Server 244 |
| `dag_master_gold` | `EXEC [dw_etl].[sp_insert_phones_factPersonalInfo]` | SQL Server 244 |

## Modulos compartidos

| Archivo | Responsabilidad |
|---|---|
| `dags/common/etl_base.py` | Motor ETL incremental para clientes, SQL externo y batch insert |
| `dags/common/etl_basephone.py` | Motor `TRUNCATE + INSERT` para telefonos |
| `dags/common/sql_loader.py` | Carga archivos `.sql` desde `dags/sql` y reemplaza parametros |
| `dags/common/audit_logger.py` | Registra auditoria en MariaDB y genera logs `.txt` |
| `dags/common/db_connections.py` | IDs de conexiones Airflow y rutas comunes |
| `dags/common/email_notifier.py` | Notificaciones SMTP para procesos operativos |

## Dependencias principales

Infraestructura:

- Docker Compose
- `postgres:13`
- `apache/airflow:2.9.3`

Paquetes Python instalados en Airflow:

- `polars`
- `connectorx`
- `sqlalchemy`
- `pymysql`
- `pyarrow`
- `apache-airflow-providers-mysql`
- `pyodbc`
- `apache-airflow-providers-odbc`
- `pymssql==2.2.11`
- `apache-airflow-providers-microsoft-mssql`

Conexiones Airflow esperadas:

| Conn ID | Uso |
|---|---|
| `MariaDB` | Origen MariaDB 242 y auditoria `flybackDW` |
| `MariaDB240` | Origen MariaDB 240 |
| `MSSQL244` | Destino SQL Server 244 |

Variables `.env` relevantes:

- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_DB`
- `EMAIL_USER`
- `EMAIL_PASSWORD`

## Mejoras implementadas frente a SSIS

- DAGs individuales ejecutables ante fallos parciales.
- Reintentos automaticos en pipelines Bronze.
- Auditoria por Airflow, tabla `etl_audit_log` y archivos `.txt`.
- Codigo Python versionable en Git.
- SQL de clientes separado en archivos `.sql`.
- Insercion batch con `executemany` para reducir roundtrips.
- Monitoreo desde Airflow UI sin depender del servidor Windows/SSIS.

## Observaciones tecnicas

- `dag_master_gold` actualmente corre por horario; el sensor de dependencia con Bronze esta comentado.
- `plugins/etl` mantiene versiones paralelas/antiguas de helpers que tambien existen en `dags/common`.
- Existen archivos `.bk` y `__pycache__` dentro de `dags`; conviene excluirlos del despliegue productivo.
- Varios documentos historicos tenian problemas de encoding; este README se deja en ASCII para evitar mojibake.

## Proximos pasos recomendados

1. Consolidar `dags/common` como unica fuente de verdad para helpers ETL.
2. Externalizar tambien SQL de telefonos para dejar cero SQL embebido en Bronze.
3. Reactivar dependencia formal Bronze -> Gold.
4. Crear una imagen Airflow propia con dependencias preinstaladas.
5. Agregar prueba de parseo de DAGs y prueba de carga de SQL externo.
