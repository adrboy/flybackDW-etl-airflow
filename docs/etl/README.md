# ETL Bronze → Gold — Documentación del Proyecto

**Proyecto:** Migración ETL SSIS → Apache Airflow  
**Autor:** Andrés — Gusacapital  
**Fecha inicio:** Mayo 2026  
**Última actualización:** 2026-08-21  
**Estado:** ✅ Homologación v2 completada — clients, phones y gold

---

## Resumen

Migración de 11 pipelines ETL desde **Microsoft SSIS** hacia **Apache Airflow 2.9.3** (Docker), conectando **MariaDB 242/240** como origen y **SQL Server 244** como destino, con arquitectura de 3 capas: Bronze Clients → Bronze Phones → Gold.

---

## Arquitectura

### As-Is — Microsoft SSIS

```
pkgETL_MAESTRO_DW.dtsx
    │
    ├──► pkgClients42.dtsx        ← Clientes servidor 242
    │         ├── Flyback MAX      → Insert Flyback
    │         ├── BuyBack MAX      → Insert BuyBack
    │         └── MasterLink MAX   → Insert MasterLink
    │
    ├──► pkgClients40.dtsx        ← Clientes servidor 240
    │         ├── Vacation C MAX   → Insert Vacation C
    │         └── Financiamiento   → Insert Financiamiento
    │
    └──► pkgPhone.dtsx            ← Teléfonos todos los productos
              ├── Limpieza + Insert Phone FB
              ├── Limpieza + Insert Phone BuyBack
              ├── Limpieza + Insert Phone MasterLink
              ├── Limpieza + Insert Phone Financiamiento
              └── Limpieza + Insert Phone Vacation Center
```

### To-Be — Apache Airflow

```
dag_masterclients   ← Bronze Clients (INCREMENTAL — clientid > max_id)
    ├── dag_clientsfi_240   ✅ v2
    ├── dag_clientsvc_240   ✅ v2
    ├── dag_clientsfb_242   ✅ v2
    ├── dag_clientsbb_242   ✅ v2  ← estándar de oro
    └── dag_clientsml_242   ✅ v2

dag_masterphones    ← Bronze Phones (TRUNCATE + INSERT)
    ├── dag_phonefi_240     ✅ v2.1
    ├── dag_phonevc_240     ✅ v2.1
    ├── dag_phonefb_242     ✅ v2.1
    ├── dag_phonebb_242     ✅ v2.1
    └── dag_phoneml_242     ✅ v2.1

dag_master_gold     ← Gold Layer (SPs SQL Server) ✅ v2.2
    ├── sp_etl_maestro
    └── sp_insert_phones_factPersonalInfo
```

---

## Patrón v2 — Clientes (INCREMENTAL)

Todo DAG de clients sigue este patrón desde `dag_clientsbb_242.py` (19/06/2026).

### Estructura de archivos

```
dags/
├── etl/
│   └── dag_clientsXX_XXX.py        ← DAG — solo orquestación
├── sql/
│   └── clients/
│       ├── get_max_id.sql           ← SELECT MAX(clientid) externo
│       ├── select_clientsXX_XXX.sql ← SELECT origen con {max_id}
│       ├── insert_clientsXX_XXX.sql ← INSERT destino con %s
│       ├── exec_sp_maestro.sql      ← EXEC SP Gold Layer
│       └── exec_sp_phones.sql       ← EXEC SP Gold Layer phones
└── common/
    ├── etl_base.py      v2.4        ← Motor ETL — cero SQL embebido
    ├── audit_logger.py              ← Log a MariaDB + archivo .txt
    ├── db_connections.py            ← Connection IDs centralizados
    └── sql_loader.py                ← Carga archivos .sql externos
```

### Reglas del patrón clients

- **Cero SQL en Python** — todo SQL vive en archivos `.sql` externos
- **`.bk` obligatorio** antes de modificar cualquier archivo
- **`dag_id` como primer parámetro** en `ejecutar_insert()` para trazabilidad
- **`BATCH_SIZE = 1000`** — lotes de 1000 filas con `executemany`
- **`estado = "ERROR"` por defecto** — solo cambia a `"SUCCESS"` si completa
- **`finally` siempre loguea** — Airflow nunca se queda sin auditoría

### Firma de etl_base.py v2.4

```python
# get_max_id — SQL externo
get_max_id(mssql_conn_id, tabla_destino)
    → lee: sql/clients/get_max_id.sql

# ejecutar_insert — motor batch incremental
ejecutar_insert(
    dag_id          : str
  , mariadb_conn_id : str
  , mssql_conn_id   : str
  , sql_select      : str   # ruta relativa al .sql de SELECT
  , sql_insert      : str   # ruta relativa al .sql de INSERT
  , max_id          : int
  , etl_fecha       : datetime = None
) -> int  # filas insertadas
```

---

## Patrón v2.1 — Phones (TRUNCATE + INSERT)

Todo DAG de phones sigue este patrón desde 22/06/2026.

### Estructura de archivos

```
dags/
├── etl/
│   └── dag_phoneXX_XXX.py          ← DAG — solo orquestación
├── sql/
│   └── phones/
│       ├── select_phone.sql         ← SELECT clientid, PHONE con {vista_origen}
│       ├── insert_phone.sql         ← INSERT con {tabla_destino} y %s
│       └── truncate_phone.sql       ← TRUNCATE con {tabla_destino}
└── common/
    └── etl_basephone.py  v2.3       ← Motor TRUNCATE+INSERT
```

### Reglas del patrón phones

- **TRUNCATE + INSERT** — tabla completa en cada ejecución
- **Transacción única** — commit al final, rollback si falla en cualquier lote
- **Un solo SQL select/insert/truncate compartido** — mismo archivo para los 5 productos
- **`dag_id` como primer parámetro** — identidad en logs
- **`BATCH_SIZE = 1000`** — lotes de 1000 filas con `executemany`
- **`.bk` obligatorio** antes de modificar cualquier archivo

### Firma de etl_basephone.py v2.3

```python
ejecutar_truncate_insert(
    dag_id          : str
  , mariadb_conn_id : str
  , mssql_conn_id   : str
  , vista_origen    : str   # db_general.vwpersonalinfo{xx}
  , tabla_destino   : str   # source.Phone{xx}
) -> int  # filas insertadas
```

### Flujo de transacción

```
TRUNCATE TABLE {tabla_destino}  → commit inmediato
    ↓
SELECT clientid, PHONE FROM {vista_origen}
    ↓
executemany lote 1..N           → SIN commit por lote
    ↓
COMMIT único al final           → todo insertado o nada
    ↓ (si falla en cualquier lote)
Log: lote fallido + filas procesadas + traceback
ROLLBACK                        → tabla queda intacta
```

---

## Patrón v2.2 — Gold Layer (SQLExecuteQueryOperator)

### Regla crítica — template_searchpath obligatorio

```python
# SIEMPRE incluir en DAGs que usen SQLExecuteQueryOperator con .sql externos
with DAG(
    ...
  , template_searchpath = "/opt/airflow/dags"  # ← Jinja2 busca desde aquí
) as dag:
    tarea = SQLExecuteQueryOperator(
        sql = "sql/clients/exec_sp_maestro.sql"  # ← ruta relativa a dags/
    )
```

**Sin `template_searchpath`** Jinja2 busca el template en la carpeta del DAG
(`dags/etl/`) y lanza `TemplateNotFound`. El error es silencioso y confuso.

**Por qué no SQL embebido:**
```python
# ❌ NUNCA — SQL embebido
sql = "EXEC [dw_etl].[sp_etl_maestro]"

# ✅ SIEMPRE — SQL externo
sql = "sql/clients/exec_sp_maestro.sql"
```

### Estructura de archivos gold

```
dags/
├── etl/
│   └── dag_master_gold.py          ← DAG con template_searchpath
└── sql/
    └── clients/                    ← dominio clientes (bronze + gold)
        ├── exec_sp_maestro.sql     ← EXEC sp_etl_maestro
        └── exec_sp_phones.sql      ← EXEC sp_insert_phones_factPersonalInfo
```

---

## Estructura y características de la programación de las extracciones (RAW — CDC diario)

> **Alcance:** DAG `dag_raw_clients_cdc_diario` — extracción incremental de clientes desde los orígenes on-premises hacia RAW (MariaDB `db_general`).
> **Documentado:** 2026-10-20. Silver (SQL Server) y Gold no se describen aquí.

### ¿Qué tiene la programación?

**Piezas: tres capas de código y los SP**

| Capa | Archivo | Responsabilidad |
|---|---|---|
| DAG | `dags/etl/dag_raw_clients_cdc_diario.py` | Horario, tareas y reintentos. Solo orquesta |
| Negocio | `dags/etl/etl_raw_ne.py` (`orquestar`) | Recorre los SP de una instancia, captura errores, escribe auditoría, TXT y correo |
| Datos | `dags/common/etl_raw_da.py` | Lista de SP por instancia, conexiones, `ejecutar_sp` y `registrar_log` |
| SP | MariaDB `db_general.etl_clients*` | Hace el CDC: INSERT de nuevos + UPDATE de modificados |

La lógica del CDC vive en los SP. Python solo hace el `CALL` de cada uno.

**Flujo**

```
08:00 y 18:00 (cada día)
        │
        ▼
dag_raw_clients_cdc_diario
   ├─► cdc_instancia_242 ─► etl_clientsfb ─► etl_clientsbb ─► etl_clientsml
   │        (las dos instancias corren en paralelo, sin dependencia entre ellas)
   └─► cdc_instancia_240 ─► etl_clientsfi ─► etl_clientsvc

   Por cada instancia, al terminar: auditoría en BD + reporte TXT + correo
```

**Características**

- **Paralelismo:** dos tareas independientes, una por instancia MariaDB, que corren a la vez.
- **Secuencia interna:** dentro de cada instancia los SP corren uno tras otro, así que hay una carga a la vez por servidor.
- **Aislamiento de fallos:** si un SP falla, los siguientes igual se ejecutan. La tarea falla al final si alguno falló.
- **Reintentos:** 2, con 60 s de espera. Un reintento repite todos los SP de la instancia, y son idempotentes.
- **Una sola corrida activa:** `max_active_runs = 1`. Un disparo extra no corre en paralelo: queda en cola (`queued`) y se ejecuta cuando termina el anterior.
- **`catchup = False`:** si Airflow estuvo apagado, solo se ejecuta la última corrida perdida.
- **Lógica incremental:** el INSERT toma solo claves mayores al máximo de RAW, y el UPDATE solo toca filas cuyas columnas cambiaron.
- **`updatedAt`:** en fb, bb y ml se actualiza solo (`ON UPDATE CURRENT_TIMESTAMP`), y eso permite a Silver detectar cambios.

**Tipos de log**

| Log | Dónde | Quién lo escribe | Contenido |
|---|---|---|---|
| Auditoría del SP | `db_general.etl_audit_log` (los cinco; fb, bb y ml desde su v1.1, desplegada el 2026-10-20). El histórico de fb, bb y ml anterior a la v1.1 está en `flybackDW.etl_audit_log`. `paquete` = nombre del SP | El propio SP | `RUNNING` → `OK` o `ERROR`, filas y mensaje de error |
| Auditoría del DAG | `db_general.etl_audit_log` en las dos instancias, `paquete` = `dag_raw_clients_cdc_diario`, `tipo_ejecucion` = `DIARIO-CDC` | `registrar_log` (Python) | Una fila por SP: `OK` si la llamada no lanzó excepción |
| Reporte TXT | `logs/etl_raw_clients_{instancia}_FB_log_{AAAAMMDDHHMMSS}.txt` | `orquestar` | Resultado por SP de la instancia |
| Correo | SMTP corporativo | `send_etl_notification` | Un correo por instancia y por corrida |
| Log de tarea | UI de Airflow y `logs/dag_id=...` | Airflow | Salida estándar y traceback |

El `FB` del nombre del TXT es un sufijo fijo, no indica la fuente.

### ¿Para qué se usa este tipo de programación?

**Propósito:** mantener RAW al día con los cambios del origen, dos veces al día y sin recargar todo, dejando evidencia verificable de cada ejecución. RAW es la base sobre la que leen Silver y Gold.

**Por qué está hecha así**

- **El CDC en SP:** la lógica corre junto a los datos y se puede probar o repetir a mano con un `CALL`.
- **Python solo orquesta:** separar DAG, negocio y datos permite cambiar horario, reintentos o avisos sin tocar la lógica de extracción.
- **Una tarea por instancia:** los orígenes están en servidores distintos, así que una caída en uno no frena al otro y el tiempo total es el de la instancia más lenta.
- **Un fallo no detiene al resto:** una fuente con problemas no bloquea a las demás.
- **Idempotente:** repetir una corrida o reintentarla es seguro.
- **Auditoría en tres niveles (BD, TXT y correo):** se puede saber qué pasó sin entrar al servidor.

### ¿Cómo se ejecuta?

- **Automática:** 08:00 y 18:00, hora `America/Cancun`.
- **DAG manual:** desde la UI de Airflow (Trigger DAG) o, dentro del contenedor, `airflow dags trigger dag_raw_clients_cdc_diario`.
- **Un SP a mano:** `CALL db_general.etl_clientsfb();` (fb, bb y ml en la 242; fi y vc en la 240).

**Cómo verificar una corrida**

1. Estado del DAG en Airflow.
2. El TXT más reciente de cada instancia.
3. `etl_audit_log`. El OK de un cliente SQL o del DAG no basta, porque los SP anotan sus errores en esa tabla.

```sql
SELECT id, paquete, tipo_ejecucion, estado, filas_insertadas, mensaje_error, fecha_inicio
FROM   db_general.etl_audit_log
WHERE  estado <> 'OK'
ORDER BY id DESC
LIMIT 20;
```

Una consulta vacía significa que ninguna ejecución registró error. Se corre en la 240 y en la 242. Para el histórico de fb, bb y ml anterior a la v1.1, consulta también `flybackDW.etl_audit_log` con `AND paquete LIKE 'etl_clients%'` y `estado NOT IN ('OK', 'SUCCESS')`: ahí quedaron los errores del 2026-10-05 y 2026-10-12.

### Candado `GET_LOCK` en los SP del CDC

**¿Qué es?** Cada SP del CDC pide, al empezar, un candado con nombre en MariaDB (`GET_LOCK`) y lo suelta al terminar. Mientras alguien lo tiene, cualquier otra ejecución del mismo SP espera. Funciona como un semáforo: **una sola ejecución a la vez de cada SP**.

**¿Por qué existe?** Antes de la v1.1, dos ejecuciones solapadas leían el mismo `MAX(clave)` de RAW e intentaban insertar los mismos clientes nuevos. Una ganaba y la otra fallaba con `Duplicate entry` en la llave primaria. Quedó registrado en fi (2026-10-08 y 2026-10-12) y en fb (2026-10-05 y 2026-10-12). Los TXT del 8 y del 12 de octubre muestran dos corridas del DAG a segundos de diferencia; que el solape fue la causa se infiere, no se probó directamente.

**¿Para qué sirve?**

- Evita el `Duplicate entry` cuando dos ejecuciones coinciden.
- Cubre un `CALL` manual desde Navicat mientras el DAG corre, cosa que `max_active_runs = 1` no ve.

**¿Cómo funciona?**

```sql
IF COALESCE(GET_LOCK('db_general.etl_clientsfi', 300), 0) <> 1 THEN
    SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'etl_clientsfi sigue ocupado por otra ejecucion; reintentar';
END IF;
-- ... el trabajo del SP ...
DO RELEASE_LOCK('db_general.etl_clientsfi');
```

- `GET_LOCK(nombre, 300)` devuelve 1 si lo obtiene, 0 si pasaron 300 s sin conseguirlo y NULL si hubo error. `COALESCE(..., 0)` trata el NULL como fallo.
- El 300 es el **máximo** de espera, no un tiempo fijo: si el candado se libera a los 6 s, el SP continúa a los 6 s.
- Se pide **antes** de leer `MAX(clave)` y de escribir en `etl_audit_log`, así la segunda ejecución ya ve lo que dejó la primera.
- Se libera de tres formas: al terminar el SP (`DO RELEASE_LOCK`), en el handler de error antes del `RESIGNAL`, y automáticamente si la conexión se cierra (Python cierra la conexión en `ejecutar_sp`).
- Los candados valen para todo el servidor MariaDB, no por esquema. Por eso el nombre lleva el prefijo `db_general.`:

| SP | Candado | Instancia |
|---|---|---|
| `etl_clientsfb` | `db_general.etl_clientsfb` | 242 |
| `etl_clientsbb` | `db_general.etl_clientsbb` | 242 |
| `etl_clientsml` | `db_general.etl_clientsml` | 242 |
| `etl_clientsfi` | `db_general.etl_clientsfi` | 240 |
| `etl_clientsvc` | `db_general.etl_clientsvc` | 240 |

**¿Qué pasa con la segunda ejecución?**

| Situación | Resultado |
|---|---|
| La primera termina antes de 300 s | La segunda continúa, no encuentra nada nuevo (es idempotente) y termina `OK` con 0 filas |
| La primera falló | La segunda hace el trabajo normal |
| Pasan 300 s sin conseguir el candado | La segunda falla con `sigue ocupado por otra ejecucion; reintentar`. En el DAG la tarea reintenta a los 60 s (`retries = 2`). Falla antes de crear su fila de auditoría, así que no deja fila `ERROR` en `etl_audit_log` (sin probar) |

**Dos capas distintas de espera**

| Capa | Qué pasa con la segunda ejecución | Cómo se ve |
|---|---|---|
| DAG de Airflow | Espera en cola hasta que termine la primera (`max_active_runs = 1`) | Estado `queued` en la UI |
| SP en MariaDB | Espera dentro de la base, bloqueada en `GET_LOCK` | La tarea sigue `running`, o el `CALL` de Navicat queda en «Procesando» |

**Cómo operarlo**

```sql
-- ¿Quién tiene el candado? NULL = libre; un número = id de conexión de quien lo tiene
SELECT IS_USED_LOCK('db_general.etl_clientsbb');

-- Los cinco juntos (los de fb, bb y ml en la 242; los de fi y vc en la 240)
SELECT IS_USED_LOCK('db_general.etl_clientsfb') AS fb
     , IS_USED_LOCK('db_general.etl_clientsbb') AS bb
     , IS_USED_LOCK('db_general.etl_clientsml') AS ml;
SELECT IS_USED_LOCK('db_general.etl_clientsfi') AS fi
     , IS_USED_LOCK('db_general.etl_clientsvc') AS vc;

-- Soltarlo desde la sesión que lo tiene
SELECT RELEASE_LOCK('db_general.etl_clientsbb');
```

**Cómo probarlo sin tocar producción.** Con un nombre inventado y dos pestañas distintas (comprobar que los dos `CONNECTION_ID()` son diferentes):

```sql
-- Pestaña A
SELECT GET_LOCK('prueba_candado', 0);        -- 1
-- Pestaña B: debe quedarse esperando
SELECT GET_LOCK('prueba_candado', 60);       -- espera; devuelve 1 cuando A suelta, o 0 a los 60 s
-- Pestaña A
SELECT RELEASE_LOCK('prueba_candado');       -- 1
```

**Precauciones (observadas el 2026-10-20)**

- **Probar siempre con un nombre inventado, nunca con el de un SP real.** En una prueba con el candado de bb, un `CALL` manual esperó 72 s y hubo que detenerlo.
- Una misma sesión puede quedar con el candado tomado más de una vez: después de un `RELEASE_LOCK` con resultado 1, `IS_USED_LOCK` seguía mostrando a esa sesión. Si pasa, repetir `RELEASE_LOCK` hasta que dé `NULL`, o cerrar la conexión. La causa probable es que el candado se cuenta por cada toma; no se verificó. Cerrar la pestaña de Navicat puede no cerrar la conexión.
- Antes de las 08:00 y las 18:00, `IS_USED_LOCK` de los cinco debe dar `NULL`. Un candado olvidado haría esperar 300 s al CDC, y la tarea fallaría.

**Estado de la prueba**

| Qué | Estado |
|---|---|
| El candado hace esperar a la segunda ejecución | Probado con un `CALL` real de bb: esperó 38 s y 72 s |
| La espera continúa al soltarlo | Probado con un candado de prueba: devolvió 1 a los 6.6 s |
| Vencimiento a los 300 s (`SIGNAL` y `RESIGNAL`) | Sin probar |

### Estado de la implementación (2026-10-20)

| Tema | Estado |
|---|---|
| `RESIGNAL` y candado `GET_LOCK` en los SP del CDC | Desplegados el 2026-10-20: fi (v1.2) y fb, bb, vc y ml (v1.1). Comparados contra el repo con `diff`, sin diferencias: fi, bb y vc. fb y ml verificados por palabras clave (`GET_LOCK` y `RESIGNAL` presentes). Primeras corridas del DAG con los cinco SP: 2026-10-20 18:00 y 2026-10-21 08:00, todas `OK` en `db_general.etl_audit_log` (20 filas, 12 en la 242 y 8 en la 240, sin errores ni `RUNNING` colgados). Los caminos de error del `RESIGNAL` y el vencimiento del candado siguen sin verse en una corrida real. Archivos en `dags/sql/clients/sp/raw_bronce/etl_clients*_cdc.sql`. El candado hace que una segunda ejecución del mismo SP, incluso un `CALL` manual, espere hasta 300 s a que termine la primera |
| Límite de corridas simultáneas (`max_active_runs`) | `max_active_runs = 1` desde 2026-10-20. Los disparos extra quedan en cola y se ejecutan después. Descartarlos en lugar de encolarlos queda como mejora pendiente |
| Definiciones de los SP en el repo | CDC: los cinco, en `dags/sql/clients/sp/raw_bronce/`. Borrado: los cinco, en la misma carpeta |

**Limitaciones conocidas**

- Antes del 2026-10-20 el DAG podía reportar OK con un SP fallido. Está demostrado con fi el 2026-10-12: error registrado en `etl_audit_log` y TXT con ✅. fb registró errores `Duplicate entry` el 2026-10-05 y el 2026-10-12. Con los cinco SP en v1.1 o posterior el error se relanza y llega al DAG; falta verlo en una corrida real.
- Un disparo extra se encola y repite el trabajo, aunque sea idempotente. Antes del 2026-10-20 no había límite: el 2026-10-08 y el 2026-10-12 hubo dos corridas a segundos de diferencia, y coincidieron con errores `Duplicate entry` en la llave de `tbl_raw_clientsfi`.
- Si una corrida se queda colgada, las siguientes esperan en cola. No hay `dagrun_timeout` configurado.
- Existe una fila `RUNNING` huérfana del 2026-09-24 en la 240. No afecta la operación.

### Organización de `dags/sql/clients`

Los archivos SQL se separan según quién los usa:

```
dags/sql/clients/
├── (raíz)        SQL que carga Python con `sql_loader.cargar_sql`:
│                  select_* / insert_* / update_* de Silver, get_max_id,
│                  insert_audit_log_raw y exec_sp_* (EXEC de Gold que lanza Airflow)
└── sp/            Definiciones de stored procedures (scripts de despliegue)
    ├── raw_bronce/ SP de RAW en MariaDB: CDC (`etl_clientsXX_cdc.sql`) y borrado (`etl_clientsXX_delete.sql`)
    ├── silver/    reservada para SP de Silver (vacía hoy)
    └── gold/      reservada para SP de Gold en SQL Server (vacía hoy)
```

- **Python no carga nada de `sp/`.** Los SP se despliegan a mano (DROP + CREATE) y Python solo los llama por nombre con `CALL`.
- **El archivo del repo es la versión vigente del SP**, aunque ningún DAG lo lea. Se despliega ejecutando el archivo, no editando el SP dentro de la base, para que repo y base no se desfasen.

Fuera de alcance de esta sección: `dag_raw_clients_delete_mensual` (día 3, 03:00) y `dag_silver_clients_quincenal` (días 1 y 15, 06:00).

---

## Conexiones Airflow

| ID | Tipo | Servidor |
|---|---|---|
| `MariaDB` | MySQL | 192.168.10.242 |
| `MariaDB240` | MySQL | 192.168.10.240 |
| `MSSQL244` | MSSQL | 192.168.10.244 |

---

## Historial de versiones

### etl_base.py (clients)

| Versión | Fecha | Cambio |
|---|---|---|
| v1.0 | May 2026 | Motor inicial — conexiones directas pymssql |
| v2.0 | May 2026 | Hooks de Airflow — sin credenciales en código |
| v2.1 | Jun 2026 | NULL → None para compatibilidad pymssql |
| v2.2 | 19/06/2026 | dag_id + traceback + blindaje conexiones finally |
| v2.3 | 22/06/2026 | get_max_id → SQL externo, cero SQL embebido |
| v2.4 | 22/06/2026 | Blindaje conexiones + log detallado en except |
| v3.0 | 19/08/2026 | pyodbc + fast_executemany — rendimiento máximo |
| v3.1 | 20/08/2026 | `_diagnosticar_lote` — diagnóstico fila/columna en except |

### etl_basephone.py (phones)

| Versión | Fecha | Cambio |
|---|---|---|
| v1.0 | May 2026 | Motor inicial — execute row-by-row, SQL embebido |
| v2.0 | 22/06/2026 | executemany + SQL externo + dag_id + traceback |
| v2.1 | 22/06/2026 | Transacción única — commit al final, rollback si falla |
| v2.2 | 22/06/2026 | Log detallado ANTES del rollback |
| v2.3 | 22/06/2026 | TRUNCATE → SQL externo + rollback seguro con None check |

### dag_master_gold.py

| Versión | Fecha | Cambio |
|---|---|---|
| v1.0 | May 2026 | MsSqlOperator — SQL embebido |
| v2.0 | 22/06/2026 | SQLExecuteQueryOperator + SQL externo |
| v2.1 | 22/06/2026 | template_searchpath corregido |
| v2.2 | 22/06/2026 | template_searchpath = "/opt/airflow/dags" — definitivo |

---

## Auditoría de sincronización — 22/06/2026

### Clients

| DAG | MAX origen | MAX destino | Registros insertados | Estado |
|---|---|---|---|---|
| dag_clientsvc_240 | 57,765 | 57,765 | 48 | ✅ |
| dag_clientsfb_242 | ~373,927 | ~373,924 | 906 | ✅ |
| dag_clientsml_242 | 797 | 797 | 0 | ✅ |

**Nota:** Se eliminaron 111 registros con clientid `999190748–999190858` de `source.clientsfb` — carga de prueba del 15/06/2026.

### Phones

| DAG | Origen | Destino | Estado |
|---|---|---|---|
| dag_phonefb_242 | 473,843 | 473,905 | ✅ |
| dag_phonebb_242 | 11,340 | 11,340 | ✅ |
| dag_phoneml_242 | 862 | 862 | ✅ |
| dag_phonefi_240 | 94,597 | 94,604 | ✅ |
| dag_phonevc_240 | 93,977 | 93,983 | ✅ |

### Gold

| DAG | Duración | Estado |
|---|---|---|
| dag_master_gold | 15 seg | ✅ |

---

## Data Dictionary

Contrato de columnas por DAG — origen MariaDB → Silver SQL Server.

| DAG | Archivo |
|---|---|
| `dag_clientsfi_240` | [data_dictionary/clients/clientsfi_240.md](data_dictionary/clients/clientsfi_240.md) |
| `dag_clientsvc_240` | [data_dictionary/clients/clientsvc_240.md](data_dictionary/clients/clientsvc_240.md) |
| `dag_clientsfb_242` | [data_dictionary/clients/clientsfb_242.md](data_dictionary/clients/clientsfb_242.md) |
| `dag_clientsbb_242` | [data_dictionary/clients/clientsbb_242.md](data_dictionary/clients/clientsbb_242.md) |
| `dag_clientsml_242` | [data_dictionary/clients/clientsml_242.md](data_dictionary/clients/clientsml_242.md) |
| Bronze & Gold Layer | [data_dictionary/bronze_layer.md](data_dictionary/bronze_layer.md) |

---

## Pendientes

- [ ] Profiling `time.perf_counter()` en `etl_basephone.py` — mañana
- [ ] Migrar `pymssql` → `pyodbc` + `fast_executemany` si profiling confirma cuello en INSERT
- [ ] Investigar 5 huérfanos en `source.clientsfb` (clientid <= 372,993)
- [ ] Investigar 3 huérfanos en `source.clientsml` (clientid <= 797)
- [ ] Refactor batch — dag_factory + BatchResult (discutir con Gemini)
- [ ] UPSERT phones cuando origen tenga llave natural por teléfono

---

## Mejoras implementadas vs SSIS

- Auditoría triple: Airflow UI + `flybackDW.etl_audit_log` + archivos `.txt`
- Reintentos automáticos: 3 intentos con 60 segundos de pausa
- DAGs individuales ejecutables ante fallos parciales
- Monitoreo desde UI web sin acceso al servidor
- SQL externalizado — modificable sin tocar Python
- Transacción única con rollback en phones — tabla nunca queda a medias
- `template_searchpath` documentado — regla de oro para Gold Layer
- Versionable en Git con historial de `.bk`
- Open Source — sin licencias adicionales
