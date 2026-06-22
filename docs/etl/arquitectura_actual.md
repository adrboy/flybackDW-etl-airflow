# Arquitectura actual del proyecto DockersETL

Fecha de analisis: 2026-06-22  
Repositorio: `DockersETL`  
Objetivo del proyecto: migrar y operar pipelines ETL originalmente ejecutados en SSIS hacia Apache Airflow sobre Docker, integrando origenes MariaDB 240/242 con SQL Server 244 y procesos auxiliares en MariaDB `flybackDW`.

## 1. Resumen ejecutivo

El proyecto implementa una plataforma ETL local basada en Docker Compose y Apache Airflow 2.9.3. La arquitectura actual contiene:

- Airflow Webserver y Scheduler ejecutandose en contenedores.
- PostgreSQL 13 dedicado como metadata database de Airflow.
- DAGs Python montados desde `./dags`.
- Modulos comunes para conexion, auditoria, carga SQL, email y motores ETL reutilizables.
- Pipelines Bronze para clientes y telefonos.
- Pipeline Gold basado en stored procedures de SQL Server.
- DAGs adicionales para procedimientos operativos de `flybackDW` en MariaDB.
- Documentacion funcional y tecnica en `docs`.

La base arquitectonica es adecuada para una migracion incremental desde SSIS: separa orquestacion, acceso a datos, SQL externo y auditoria. Los principales riesgos actuales estan en higiene del repositorio, duplicacion de modulos, manejo de secretos, dependencias instaladas dinamicamente en arranque y falta de pruebas automatizadas de DAGs/SQL.

## 2. Vista de arquitectura

```text
Usuario / Operacion
        |
        v
Airflow UI :8085
        |
        v
airflow-webserver + airflow-scheduler
        |
        +-- Metadata Airflow --> postgres:13
        |
        +-- DAGs Python --> ./dags
        |
        +-- Logs --> ./logs
        |
        +-- Plugins --> ./plugins
        |
        +-- Data lake local --> ./data_lake
        |
        +-- Origenes MariaDB --> MariaDB 240 / MariaDB 242
        |
        +-- Destino DW --> SQL Server 244
        |
        +-- Email SMTP --> mail.gusacapital.com:587
```

## 3. Componentes principales

### 3.1 Infraestructura Docker

Archivo principal: `docker-compose.yml`.

Servicios:

- `postgres`: base interna de Airflow, imagen `postgres:13`, puerto host `5433`.
- `airflow-webserver`: imagen `apache/airflow:2.9.3`, puerto host `8085`.
- `airflow-scheduler`: misma imagen de Airflow, ejecuta el scheduler.

Volumenes montados:

- `./dags:/opt/airflow/dags`
- `./logs:/opt/airflow/logs`
- `./plugins:/opt/airflow/plugins`
- `./data_lake:/opt/airflow/data_lake`
- `./postgres_data:/var/lib/postgresql/data`

Configuracion relevante:

- Executor: `LocalExecutor`.
- Timezone: `America/Cancun`.
- Dependencias Python instaladas por `_PIP_ADDITIONAL_REQUIREMENTS`.
- Credenciales de Postgres y email tomadas desde `.env`.

### 3.2 Orquestacion Airflow

Los DAGs se organizan en tres grupos:

- DAGs Bronze en `dags/etl`.
- DAGs operativos de Flyback en `dags/etl_flyback` y raiz de `dags`.
- DAGs de prueba/certificacion en `dags`.

Los DAGs Bronze siguen dos patrones:

- Clientes: carga incremental basada en `MAX(clientid)` del destino.
- Telefonos: `TRUNCATE TABLE` del destino y recarga completa.

Los DAGs maestros usan `TriggerDagRunOperator` con `wait_for_completion=True` para ejecutar DAGs hijos de forma secuencial.

### 3.3 Capa Bronze: clientes

DAG maestro:

- `dag_masterclients`: corre semanalmente los lunes a las 06:00.

DAGs hijos:

- `dag_clientsfi_240`
- `dag_clientsvc_240`
- `dag_clientsfb_242`
- `dag_clientsbb_242`
- `dag_clientsml_242`

Flujo tecnico:

1. Lee `MAX(clientid)` en SQL Server destino.
2. Carga SQL externo cuando aplica desde `dags/sql/clients`.
3. Ejecuta SELECT en MariaDB con filtro `clientid > max_id`.
4. Inserta en SQL Server por lotes de 1,000 registros.
5. Agrega columnas de auditoria `createdAt`, `updatedAt`, `deletedAt`.
6. Registra auditoria en tabla `flybackDW.etl_audit_log` y archivo `.txt`.

### 3.4 Capa Bronze: telefonos

DAG maestro:

- `dag_masterphones`: corre semanalmente los lunes a las 06:00.

DAGs hijos:

- `dag_phonefi_240`
- `dag_phonevc_240`
- `dag_phonefb_242`
- `dag_phonebb_242`
- `dag_phoneml_242`

Flujo tecnico:

1. Trunca tabla destino en SQL Server.
2. Lee `clientid, PHONE` desde vista MariaDB.
3. Inserta por lotes de 1,000 registros.
4. Registra auditoria en MariaDB y archivo `.txt`.

### 3.5 Capa Gold

DAG:

- `dag_master_gold`: corre semanalmente los lunes a las 07:00.

Tareas:

- `EXEC [dw_etl].[sp_etl_maestro]`
- `EXEC [dw_etl].[sp_insert_phones_factPersonalInfo]`

Observacion: existe un `ExternalTaskSensor` comentado para esperar a Bronze. Actualmente Gold depende del horario, no de una dependencia activa con los DAGs Bronze.

### 3.6 Procesos Flyback / MariaDB

DAGs relevantes:

- `dag_tbInicioSolAutPag_diario`: lunes a viernes 05:30, ejecuta tres stored procedures MariaDB y notifica por email.
- `dag_limpieza_Mensual_tbInicioSolAutPag`: dia 2 de cada mes 01:00, limpieza mensual.
- `flybackDW_spInsertHistoricoCobranza`: mensual, primer dia a las 06:00.
- `flybackDW_sp_ActivosRedeemCorp`: semanal, lunes 06:00, con `catchup=True`.
- `flybackDW_sp_FinalizarContratosVencidos`: anual, 1 de febrero 06:00.

Estos procesos estan mas acoplados a stored procedures existentes en MariaDB que a la capa Bronze/Gold.

## 4. Dependencias identificadas

### 4.1 Imagenes Docker

- `postgres:13`
- `apache/airflow:2.9.3`

### 4.2 Paquetes Python instalados en Airflow

Definidos en `_PIP_ADDITIONAL_REQUIREMENTS`:

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

Dependencias importadas directamente en el codigo:

- `airflow`
- `airflow.providers.mysql`
- `airflow.providers.microsoft.mssql`
- `pymysql`
- `smtplib`
- `email.mime`
- `os`, `datetime`, `traceback`, `pathlib`, `time`

### 4.3 Sistemas externos

- MariaDB 242: conexion Airflow `MariaDB`.
- MariaDB 240: conexion Airflow `MariaDB240`.
- SQL Server 244: conexion Airflow `MSSQL244`.
- SMTP corporativo: `mail.gusacapital.com:587`.
- Base de auditoria: `flybackDW.etl_audit_log`.
- Stored procedures MariaDB y SQL Server usados por DAGs operativos y Gold.

## 5. Estructura de carpetas

```text
DockersETL/
|-- dags/
|   |-- common/
|   |   |-- audit_logger.py
|   |   |-- db_connections.py
|   |   |-- email_notifier.py
|   |   |-- etl_base.py
|   |   |-- etl_basephone.py
|   |   `-- sql_loader.py
|   |-- etl/
|   |   |-- dag_masterclients.py
|   |   |-- dag_masterphones.py
|   |   |-- dag_master_gold.py
|   |   |-- dag_clients*.py
|   |   `-- dag_phone*.py
|   |-- etl_flyback/
|   |   |-- dag_tbInicioSolAutPag_diario.py
|   |   |-- dag_limpieza_Mensual_tbInicioSolAutPag.py
|   |   `-- test_email.py
|   |-- sql/
|   |   `-- clients/
|   |       |-- select_clients*.sql
|   |       `-- insert_clients*.sql
|   |-- test_*.py
|   `-- flybackDW_sp*.py
|-- plugins/
|   `-- etl/
|       |-- audit_logger.py
|       |-- config.py
|       |-- db_connections.py
|       |-- etl_base.py
|       `-- etl_basephone.py
|-- docs/
|   |-- adr/
|   |-- data_dictionary/
|   |-- runbook/
|   |-- tbInicioSolAutPag/
|   |-- user_stories/
|   `-- *.md
|-- scripts/
|   |-- backup_etl.ps1
|   |-- backup_etl_v2.ps1
|   |-- cleanup_logs.ps1
|   |-- test_email.py
|   `-- test_env.py
|-- data_lake/
|-- logs/
|-- postgres_data/
|-- docker-compose.yml
|-- .env.example
|-- .gitignore
`-- README.md
```

### Carpetas y responsabilidades

| Carpeta / archivo | Responsabilidad actual | Observaciones |
|---|---|---|
| `dags/` | Codigo ejecutado por Airflow | Mezcla DAGs productivos, pruebas, backups y pycache. |
| `dags/common/` | Librerias internas para DAGs | Es el nucleo reutilizable real. |
| `dags/etl/` | Pipelines Bronze y Gold | Contiene maestros, hijos y backups `.bk`. |
| `dags/etl_flyback/` | Procesos operativos MariaDB Flyback | Incluye SPs, limpieza y notificaciones. |
| `dags/sql/` | SQL externo para pipelines | Actualmente solo cubre algunos clientes. |
| `plugins/etl/` | Copia/variante de helpers ETL | Duplica logica con `dags/common`. Riesgo de confusion. |
| `docs/` | Documentacion tecnica/funcional | Hay buena base, pero con problemas de encoding en varios archivos. |
| `scripts/` | Utilidades PowerShell/Python | Backups, limpieza de logs y pruebas manuales. |
| `postgres_data/` | Data directory de Postgres Airflow | Correcto como volumen, no debe versionarse. |
| raiz con archivos numericos/PG | Archivos internos de PostgreSQL sueltos | Riesgo de higiene y confusion operacional. |

## 6. Fortalezas tecnicas

- Migracion a Airflow con DAGs versionables y ejecutables de forma individual.
- Separacion razonable entre DAGs, helpers comunes y SQL externo.
- Uso de Airflow Hooks para conexiones principales.
- Reintentos configurados en varios DAGs criticos.
- Auditoria en tres niveles: Airflow, tabla MariaDB y logs `.txt`.
- Orquestacion master/hijo para controlar secuencia semanal.
- Documentacion ya existente: ADR, runbook, diccionario de datos e historias de usuario.
- Uso de batches de 1,000 registros para balancear memoria y roundtrips.

## 7. Riesgos tecnicos

### Alto

1. Archivos internos de PostgreSQL en la raiz del repositorio.
   - Se observan muchos archivos numericos, `PG_VERSION`, `pg_hba.conf`, `postgresql.conf`, `postmaster.pid`, etc.
   - Aunque `.gitignore` ya los contempla, su presencia en raiz aumenta el riesgo de respaldos incorrectos, confusion con `postgres_data` y exposicion accidental.

2. Manejo de secretos y credenciales historicas.
   - `.env` existe localmente, lo cual es esperado, pero hay credenciales comentadas en `db_connections.py` y `audit_logger.py`.
   - `plugins/etl/config.py` contiene placeholders de conexion.
   - Riesgo: copiar/pegar credenciales reales en codigo o documentos.

3. Dependencias instaladas dinamicamente con `_PIP_ADDITIONAL_REQUIREMENTS`.
   - Cada arranque puede instalar paquetes, haciendo el ambiente mas lento y menos reproducible.
   - Si cambia PyPI o falla red, el contenedor puede no arrancar igual.

4. Duplicacion entre `dags/common` y `plugins/etl`.
   - Existen modulos repetidos: `audit_logger.py`, `db_connections.py`, `etl_base.py`, `etl_basephone.py`.
   - Airflow carga `plugins`, y los DAGs manipulan `sys.path`; esto puede generar imports ambiguos o divergencia funcional.

5. Gold no depende activamente de Bronze.
   - `dag_master_gold` corre por horario y tiene comentado el `ExternalTaskSensor`.
   - Si Bronze tarda, falla o se ejecuta manualmente fuera de horario, Gold puede correr con datos incompletos.

### Medio

6. SQL dinamico con `format` y f-strings.
   - `sql_loader.cargar_sql` usa `.format(**params)`.
   - `get_max_id` arma `SELECT ... FROM {tabla_destino}`.
   - En el uso actual los parametros parecen constantes internas, pero no hay validacion/allowlist.

7. DAGs de prueba, backups y `__pycache__` dentro de `dags`.
   - Airflow escanea todo `dags`; archivos `.bk`, pruebas y cache pueden ensuciar parsing, UI y despliegue.

8. Reintentos no estandarizados.
   - Algunos DAGs hijos configuran `retries=3`; otros procesos no muestran una politica comun de retries, delays, timeouts o SLA.

9. Auditoria acoplada a una sola conexion MariaDB.
   - `registrar_log` usa directamente `mysql_conn_id='MariaDB'`.
   - Si el proceso viene de MariaDB240 o SQL Server, la auditoria sigue dependiendo de MariaDB242/flybackDW.

10. Email SMTP sin TLS explicito.
   - `email_notifier.py` usa puerto 587 con login, pero no llama `starttls`.
   - Si el servidor no cifra internamente, las credenciales podrian viajar expuestas.

11. Uso de `catchup=True` en DAGs operativos.
   - Algunos DAGs con stored procedures tienen `catchup=True`.
   - Puede ser deseado, pero en SPs con efectos acumulativos conviene documentar idempotencia y ventanas.

12. Codificacion de documentacion y comentarios.
   - Varios Markdown y comentarios aparecen con mojibake.
   - Esto reduce legibilidad y puede causar errores si se reutilizan textos en notificaciones.

### Bajo

13. Codigo comentado historico en modulos productivos.
   - Hay bloques grandes de implementaciones antiguas.
   - Aumenta ruido y dificulta revisiones.

14. Uso manual de `sys.path.insert`.
   - Funciona en Airflow, pero es fragil frente a cambios de layout o empaquetado.

15. Cobertura parcial de SQL externo.
   - Algunos clientes ya externalizan SQL; telefonos y otros DAGs conservan SQL inline.

## 8. Oportunidades de mejora

### Prioridad 1: estabilidad operativa

- Limpiar la raiz del repositorio y mover/remover archivos internos de PostgreSQL que no pertenezcan al workspace fuente.
- Crear una imagen Docker propia para Airflow con dependencias preinstaladas y versionadas.
- Reactivar una dependencia explicita Bronze -> Gold usando `ExternalTaskSensor`, `Dataset`, o un DAG maestro superior.
- Definir politica comun de DAGs: retries, retry_delay, execution_timeout, owner, alertas, catchup y tags.
- Sacar backups `.bk`, `__pycache__` y DAGs de prueba fuera de `dags` o excluirlos del despliegue.

### Prioridad 2: mantenibilidad

- Elegir una sola ubicacion para helpers ETL: preferentemente `dags/common` o un paquete interno instalado.
- Eliminar o archivar `plugins/etl` si ya no es fuente de verdad.
- Externalizar SQL restante, especialmente telefonos y SPs con bloques repetidos.
- Reemplazar constantes dispersas por configuracion central con allowlists de vistas/tablas.
- Reducir codigo comentado historico y conservar decisiones en ADRs.

### Prioridad 3: seguridad

- Remover credenciales comentadas del codigo y revisar historial Git si esas credenciales fueron reales.
- Usar Airflow Connections/Variables o secrets backend para secretos.
- Actualizar `email_notifier.py` para usar TLS si el servidor SMTP lo soporta.
- Validar identificadores dinamicos (`tabla_destino`, `vista_origen`, rutas SQL) contra listas permitidas.

### Prioridad 4: calidad y pruebas

- Agregar pruebas de parseo de DAGs para asegurar que Airflow puede cargar todos los DAGs.
- Agregar pruebas unitarias para `sql_loader`, construccion de SQL y validacion de parametros.
- Crear pruebas de humo para conexiones Airflow sin ejecutar cargas completas.
- Documentar contratos de datos por tabla: llave incremental, columnas esperadas, frecuencia, owner y estrategia de reproceso.
- Medir tiempos por DAG y volumen insertado para detectar regresiones.

### Prioridad 5: documentacion

- Reparar encoding de Markdown y comentarios para usar UTF-8 consistente.
- Consolidar README raiz y `docs/README.md` para evitar duplicacion.
- Mantener un inventario unico de DAGs con schedule, origen, destino, idempotencia y criticidad.
- Agregar runbook especifico para fallos comunes: conexion MariaDB, conexion SQL Server, deadlocks, SMTP, carga parcial y reproceso.

## 9. Recomendacion de roadmap

### Corto plazo

1. Limpiar raiz y `dags` de archivos generados/backups.
2. Consolidar helpers duplicados.
3. Crear `Dockerfile` de Airflow con dependencias fijas.
4. Reactivar dependencia formal entre Bronze y Gold.
5. Corregir secretos comentados y encoding de docs.

### Mediano plazo

1. Completar SQL externo para todos los pipelines.
2. Agregar pruebas de parseo de DAGs en CI o script local.
3. Crear inventario operativo de DAGs.
4. Estandarizar logs, retries y alertas.
5. Agregar validaciones de calidad de datos post-carga.

### Largo plazo

1. Empaquetar helpers como libreria interna versionada.
2. Evaluar Airflow Datasets para dependencias entre capas.
3. Incorporar data quality checks por tabla y por capa.
4. Separar ambientes dev/prod con variables y conexiones independientes.
5. Formalizar estrategia de backfill y reproceso.

## 10. Conclusion

DockersETL ya tiene una arquitectura funcional y razonablemente modular para operar la migracion SSIS -> Airflow. La direccion tecnica es buena: DAGs individuales, orquestacion maestro/hijo, hooks de Airflow, auditoria y SQL externo.

El siguiente salto de madurez no requiere rehacer la solucion, sino ordenar los bordes: reproducibilidad del contenedor, limpieza del repo, dependencia real Bronze -> Gold, eliminacion de duplicados y pruebas basicas de carga de DAGs. Con esas mejoras, el proyecto quedaria mas estable para operacion diaria y mas facil de extender cuando aparezcan nuevos pipelines.
