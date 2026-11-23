# Flujo ETL de clientes y teléfonos — RAW → Silver → Gold

Fecha: 2026-11-12 · Proyecto: `DockersETL` (flybackDW) · Airflow 2.11 · zona horaria del contenedor: America/Cancun

Cómo leer este documento: cada pieza tiene una cabecera como las de los archivos `.py`
(capa, objetivo, cuándo corre, flujo básico, qué lee y qué escribe). Las piezas van en el
orden en que corre el dato: orígenes → RAW → Silver → Gold. Al final están el calendario,
cómo ejecutar a mano y la auditoría de archivos.

---

## 0. Mapa general

```text
ORIGEN (MariaDB 242 y 240)
   │   diario 5:00 · 8:00 · 18:00   (+ día 3 a las 3:00: borrados)
   ▼
RAW   (MariaDB db_general, una copia por producto, con updatedAt y deletedAt)
   │   días 1 y 15 · clientes 6:00 · teléfonos 7:00
   ▼
SILVER (SQL Server 244, esquema source)
   │   cuando terminan las 5 fuentes de Silver (interruptor encendido)
   ▼
GOLD   (SQL Server 244, esquemas gral y dw_etl)
```

Productos: fb (Flyback) · bb (Buyback) · ml (Masterlinks) · fi (Financiamiento) · vc (Vacation Center).
Instancias: 242 = fb, bb, ml · 240 = fi, vc.

---

## 1. ORÍGENES

```text
# ═══════════════════════════════════════════════════════
# Orígenes (solo lectura para el ETL)
# Capa    : Origen
# Objetivo: aplicaciones del negocio; nadie del ETL escribe aquí
# Instancia 242: customers.fb_clients (fb) · buyback.clients (bb) · masterlinks.clients (ml)
# Instancia 240: financiamiento.clients + credits + clients_email (fi)
#                vtw.catowners + vtw.p_data (vc)
# Teléfonos    : vistas db_general.vwpersonalinfoXX (filtran 7 a 15 caracteres)
# Nota         : updatedAt se mueve solo (ON UPDATE) en todos, salvo vtw.p_data
#                (sin impacto hoy: capdata y worldid coinciden con RAW).
# ═══════════════════════════════════════════════════════
```

---

## 2. RAW — clientes

```text
# ═══════════════════════════════════════════════════════
# dag_raw_clients_cdc_diario
# Capa    : RAW · Carpeta: etl/ · Cuándo: todos los días 8:00 y 18:00
# Objetivo: copiar a RAW los clientes nuevos y modificados
# Flujo   :
#   1) cdc_instancia_242 y cdc_instancia_240 corren en paralelo
#   2) cada una ejecuta sus SP uno tras otro:
#        242: etl_clientsfb → etl_clientsbb → etl_clientsml
#        240: etl_clientsfi → etl_clientsvc
#   3) cada SP: candado GET_LOCK (una sola ejecución, espera 300 s)
#        INSERT de clientes nuevos (id mayor que el máximo de RAW, con contrato)
#        UPDATE de los que cambiaron (compara columnas + red de seguridad updatedAt)
#   4) si un SP falla, anota el error y sigue con el siguiente; al final la tarea
#      falla y Airflow reintenta (2 veces, 60 s): repetir es seguro (idempotente)
# Código  : dag_raw_clients_cdc_diario.py → etl_raw_ne.py → common/etl_raw_da.py
# SP      : sql/clients/sp/raw_bronce/etl_clientsXX_cdc.sql (v1.1, desplegados a mano en Navicat)
# Audita  : db_general.etl_audit_log de cada instancia
#           DIARIO-INSERT y DIARIO-UPDATE (los escribe el SP), DIARIO-CDC (lo escribe Python)
# Avisa   : .txt en logs/ y correo por instancia en cada corrida
# Límite conocido: el UPDATE no pone deletedAt en NULL (no revive un cliente borrado)
# ═══════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════
# dag_raw_clients_delete_mensual
# Capa    : RAW · Carpeta: etl/ · Cuándo: día 3 de cada mes, 3:00
# Objetivo: marcar con deletedAt los clientes que ya no existen en el origen
# Flujo   :
#   1) dos tareas en paralelo (242 y 240), cada una ejecuta sus SP en orden
#        etl_clientsfb_delete · bb · ml  /  etl_clientsfi_delete · vc
#   2) cada SP compara RAW contra el origen y marca deletedAt y updatedAt
#        (fi: también los que perdieron su crédito)
#   3) reintenta una vez a los 5 minutos si falla
# Código  : dag_raw_clients_delete_mensual.py → etl_raw_delete_ne.py → common/etl_raw_delete_da.py
# SP      : sql/clients/sp/raw_bronce/etl_clientsXX_delete.sql
# Audita  : etl_audit_log, tipo VERIFY-DELETE
# Nota    : nunca borra filas; el borrado es lógico
# ═══════════════════════════════════════════════════════
```

---

## 3. RAW — teléfonos

```text
# ═══════════════════════════════════════════════════════
# dag_raw_phones_diario
# Capa    : RAW · Carpeta: etl/ · Cuándo: todos los días 5:00
# Objetivo: mantener tbl_raw_phoneXX al día (nuevos, revividos y marcados)
# Flujo   :
#   1) raw_phones_instancia_242 (fb → bb → ml) y raw_phones_instancia_240 (fi → vc), en paralelo
#   2) cada SP sp_raw_phoneXX(p_max_pct = 2, p_forzar = 0):
#        candado GET_LOCK · INSERT de pares (clientid, phone) nuevos
#        revive los que vuelven · marca deletedAt a los que ya no están
#        compuerta: si marcaría más del 2% de los vivos, se detiene (error 1644 BLOQUEADO)
#   3) devuelve nuevos, revividos, marcados, vivos_final, segundos
# Código  : dag_raw_phones_diario.py → etl_raw_phone_ne.py → common/etl_raw_phone_da.py
# SP      : sql/clients/sp/raw_bronce/etl_sp_raw_phoneXX.sql
# Tablas  : db_general.tbl_raw_phoneXX (clientid, phone, updatedAt ON UPDATE, deletedAt;
#           PK clientid + phone)
# Audita  : etl_audit_log, tipo RAW-PHONE · Avisa: correo solo si hay error
# ═══════════════════════════════════════════════════════
```

---

## 4. SILVER — clientes

```text
# ═══════════════════════════════════════════════════════
# dag_clients_quincenal  (parte Silver)
# Capa    : Silver · Carpeta: etl/ · Cuándo: días 1 y 15, 6:00 · max_active_runs = 1
# Objetivo: llevar RAW a source.clientsXX en SQL Server
# Flujo   :
#   1) silver_fb, silver_bb, silver_ml, silver_fi, silver_vc, en paralelo
#   2) cada una: etl_silver_ne.orquestar_silver(fuente) → common/etl_silver_da.ejecutar_silver
#        decide solo el modo: FULL o incremental (por MAX(updatedAt) de Silver)
#        UPDATE + INSERT en UNA transacción (pyodbc); COMMIT o ROLLBACK
#        INSERT: filas vivas de RAW con id mayor que el máximo de Silver
#        UPDATE: filas de RAW con updatedAt mayor que la marca (incluye deletedAt)
#   3) errores: antes de escribir o rollback confirmado → reintenta (1 vez, 5 min)
#               estado desconocido tras el COMMIT → falla sin reintento (revisar a mano)
# SQL     : sql/clients/select_silver_clientsXX_insert|update.sql,
#           insert_silver_clientsXX.sql, update_silver_clientsXX.sql
# Destino : source.clientsXX (SQL Server 244, base DBGeneralDW)
# Avisa   : .txt y correo por fuente en cada corrida (no escribe filas de auditoría)
# Límite conocido: el UPDATE usa COALESCE(deletedAt): un borrado nunca se limpia en Silver
# ═══════════════════════════════════════════════════════
```

---

## 5. SILVER — teléfonos

```text
# ═══════════════════════════════════════════════════════
# dag_phones_quincenal  (parte Silver)
# Capa    : Silver · Carpeta: etl/ · Cuándo: días 1 y 15, 7:00 · max_active_runs = 1
# Objetivo: llevar tbl_raw_phoneXX a source.PhoneXX, incremental
# Flujo   :
#   1) cadena: silver_phone_fb → bb → ml → fi → vc (si una falla, las siguientes siguen)
#   2) cada una: etl_silver_phone_ne.orquestar_silver_phone → common/etl_silver_phone_da
#        conexión de control (MariaDB): GET_LOCK silver_phone_<fuente> + filas
#          SILVER-INSERT y SILVER-UPDATE en etl_audit_log (RUNNING → OK / ERROR / UNKNOWN)
#        marca = MAX(updatedAt) de source.PhoneXX; lee RAW con updatedAt >= marca
#        tabla temporal #delta en SQL Server (collation DATABASE_DEFAULT)
#        UPDATE por conjuntos (updatedAt y deletedAt copiados, sin COALESCE: revive)
#        INSERT por conjuntos de lo que falta; todo en UNA transacción
#   3) idempotente: la segunda ejecución seguida da 0 y 0
# SQL     : sql/phones/select_silver_phone_delta · create_silver_phone_delta ·
#           insert_silver_phone_delta · update_silver_phone · insert_silver_phone
# Destino : source.Phonefb|bb|fi|vc|ml (idphone PK, clientid, phone, createdAt, updatedAt,
#           deletedAt; única por clientid + phone)
# Reutiliza: excepciones, conexión a SQL Server y marca de agua de common/etl_silver_da.py
# Avisa   : correo por fuente en cada corrida (NOTIFICAR_SOLO_ERROR = False)
# ═══════════════════════════════════════════════════════
```

---

## 6. GOLD — clientes

```text
# ═══════════════════════════════════════════════════════
# Tarea gold_clientes  (dag_clients_quincenal)
# Capa    : Gold · Cuándo: al terminar las 5 de Silver (interruptor encendido)
# Flujo   :
#   1) decidir_gold (ShortCircuit) exige las cinco Silver OK y deja pasar si:
#        no se pidió una fuente concreta y GOLD_HABILITADO_POR_DEFECTO = True,
#        o si se pidió solo_gold
#   2) gold_clientes ejecuta sql/clients/exec_sp_maestro.sql → EXEC dw_etl.sp_etl_maestro
#      (conexión MSSQL244, sin reintentos, do_xcom_push = False, correo si falla)
# ═══════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════
# dw_etl.sp_etl_maestro  (v1.1)
# Capa    : Gold · Archivo: sql/clients/sp/gold/sp_etl_maestro.sql
# Flujo   :
#   1) sp_upsert_factClientes (v2.2): MERGE por correo desde source.clientescorreo
#        → gral.factClientes (un registro por correo); capdata siempre la más antigua
#        si falla, el maestro se detiene
#   2) cinco detalles, independientes: sp_upsert_clientsfb|bb|fi|vc|ml_factClientesDetalle (v1.1)
#        MERGE por clientid desde source.clientsXX → gral.factClientesDetalle
#        (un registro por certificado); solo clientes con correo real
#   3) si algún detalle falló, el maestro lanza un error con el resumen al final
# Audita  : dw_etl.dw_etl_audit_log (una fila por procedimiento)
# Nota    : solo inserta y actualiza; nunca borra filas
# ═══════════════════════════════════════════════════════
```

---

## 7. GOLD — teléfonos

```text
# ═══════════════════════════════════════════════════════
# dw_etl.sp_insert_phones_factPersonalInfo  (v1.2)
# Capa    : Gold · Archivo: sql/clients/sp/gold/sp_insert_phones_factPersonalInfo.sql
# Cuándo  : tarea gold_phones de dag_phones_quincenal (mismo interruptor y solo_gold)
# Lee     : teléfonos VIVOS de source.PhoneXX → idcorreo por gral.factClientesDetalle
#           (clientid * 10 + producto) y gral.factClientes
# Escribe : gral.factPersonalInfo (UNIQUE idcorreo + phone + productid)
# Flujo   :
#   1) candado sp_getapplock y una sola transacción
#   2) calcula cuántos insertaría, revivirá y retirará
#   3) @simular = 1: informa y revierte sin escribir
#   4) compuerta: si el retiro pasa de @max_pct (2%) de los vivos, se detiene sin escribir
#   5) MERGE: nuevo → INSERT · retirado que vuelve → deleteAt NULL (revive)
#             vivo en Gold que Silver ya no tiene → deleteAt = ahora · igual → nada
# Parámetros: @productid (NULL = los 5), @max_pct, @forzar, @simular
# Audita  : dw_etl.dw_etl_audit_log (insertados / revividos / retirados / vivos finales)
# Nota    : un teléfono corregido = uno retirado + uno nuevo (el número es parte de la llave);
#           los clientes sin correo real no entran a Gold (5,435 teléfonos al 2026-11-18)
# ═══════════════════════════════════════════════════════
```

---

## 8. Módulos compartidos

```text
# ═══════════════════════════════════════════════════════
# common/db_connections.py · sql_loader.py · email_notifier.py · audit_logger.py
# Capa    : utilidades · los usan el ETL de clientes/teléfonos Y los proyectos
#           etl_flyback y etl_datasync: NO se archivan.
# db_connections: ids de conexión de Airflow (MariaDB 242 y 240, MSSQL244) y LOG_PATH
# sql_loader    : cargar_sql(ruta, parámetros) lee los .sql externos
# email_notifier: send_etl_notification (correo por corrida o por error)
# audit_logger  : escribir_log_txt (reporte .txt en logs/)
# ═══════════════════════════════════════════════════════
```

---

## 9. Calendario

| Cuándo | Qué corre | Capa |
|---|---|---|
| Todos los días 5:00 | `dag_raw_phones_diario` | RAW teléfonos |
| Todos los días 8:00 y 18:00 | `dag_raw_clients_cdc_diario` | RAW clientes |
| Día 3, 3:00 | `dag_raw_clients_delete_mensual` | RAW borrados |
| Días 1 y 15, 6:00 | `dag_clients_quincenal`: Silver y Gold | Silver y Gold clientes |
| Días 1 y 15, 7:00 | `dag_phones_quincenal`: Silver y Gold | Silver y Gold teléfonos |

Clientes corre antes que teléfonos a propósito: el `idcorreo` de cada teléfono sale de
`gral.factClientesDetalle`, que el maestro de clientes deja al día.

---

## 10. Ejecutar a mano (bash del contenedor: `docker exec -it airflow_scheduler bash`)

| Qué | Comando |
|---|---|
| Todo (Silver y Gold) | `airflow dags trigger dag_clients_quincenal` |
| Solo Silver | `airflow dags trigger dag_clients_quincenal --conf '{"gold":false}'` |
| Solo Gold, sin repetir Silver | `airflow dags trigger dag_clients_quincenal --conf '{"solo_gold":true}'` |
| Una sola fuente (Gold no corre) | `airflow dags trigger dag_clients_quincenal --conf '{"fuente":"fb"}'` |
| Lo mismo para teléfonos | cambiar `dag_clients_quincenal` por `dag_phones_quincenal` |
| Ver corridas | `airflow dags list-runs -d dag_phones_quincenal` |

Gold de teléfonos desde SSMS: `EXEC dw_etl.sp_insert_phones_factPersonalInfo @simular = 1;`
(ensayo sin escribir) y sin `@simular` para ejecutar.

---

## 11. Auditoría de archivos (2026-11-12)

Método: se parte de los DAG activos y se sigue qué módulos y qué `.sql` usa cada uno.

**A. En uso (proyecto de clientes y teléfonos)**

| Tipo | Archivos |
|---|---|
| DAG (5) | `dag_raw_clients_cdc_diario` · `dag_raw_clients_delete_mensual` · `dag_raw_phones_diario` · `dag_clients_quincenal` · `dag_phones_quincenal` |
| Negocio (`etl/`) | `etl_raw_ne` · `etl_raw_delete_ne` · `etl_raw_phone_ne` · `etl_silver_ne` · `etl_silver_phone_ne` |
| Datos (`common/`) | `etl_raw_da` · `etl_raw_delete_da` · `etl_raw_phone_da` · `etl_silver_da` · `etl_silver_phone_da` |
| SQL de clientes | `insert_audit_log_raw` · `select_silver_clientsXX_insert|update` · `insert_silver_clientsXX` · `update_silver_clientsXX` (XX = fb, bb, ml, fi, vc) · `exec_sp_maestro` · `exec_sp_phones` |
| SQL de teléfonos | `insert_audit_log_raw_phone` · `create_silver_phone_delta` · `insert_silver_phone_delta` · `select_silver_phone_delta` · `update_silver_phone` · `insert_silver_phone` |
| SP (archivos de despliegue) | `sp/raw_bronce/` (cdc, delete y sp_raw_phone) y `sp/gold/` (7 de clientes y teléfonos) |

**B. Compartidos con otros proyectos (se quedan):** `audit_logger` · `email_notifier` · `db_connections` · `sql_loader`.
Verificado en `etl_flyback/dag_inicio_r_diario.py`, `flybackDW_sp_ActivosRedeemCorp.py` y `etl_datasync/datasync_master.py`.
No verificado: `flybackDW_spInsertHistoricoCobranza.py` y `flybackDW_sp_FinalizarContratosVencidos.py`.

**C. Candidatos a mover a `backup/` (solo los usaban los DAG ya retirados)**

| Archivo | Por qué |
|---|---|
| `common/etl_base.py` (+ `.bk`) | motor INCREMENTAL de los antiguos `dag_clientsXX_24x` |
| `common/error_classifier.py` | solo lo importa `etl_base.py` |
| `common/etl_basephone.py` (+ `.bk`) | motor TRUNCATE + INSERT de los antiguos `dag_phoneXX` |
| `sql/clients/get_max_id.sql` | lo usaba `etl_base` |
| `sql/clients/select_clientsXX_24x.sql` y `insert_clientsXX_24x.sql` (10 archivos) | cargas Bronze viejas |
| `sql/phones/select_phone.sql`, `insert_phone.sql`, `truncate_phone.sql` | los usaba `etl_basephone` |

**D. Otros proyectos (no se tocan):** `etl_datasync/` · `etl_flyback/` · `sql/datasync/` · `sql/etl_flyback/` · `common/db_datasync.py`.

**E. DAG de entorno, pendientes de decidir:** `test_conexion_mariadb.py` · `test_conexion_mssql244.py` · `test_librerias.py` (en la raíz de `dags`, pausados).

**F. Documentación previa por revisar:** `docs/etl/arquitectura_actual.md` (2026-06-22, Airflow 2.9.3, describe Bronze),
`bronze_layer.md`, `etl_bronze_runbook.md`, `US001_bronze_layer.md`, `README_phones.md`.

---

## 12. Pendientes conocidos

| Tema | Detalle |
|---|---|
| Países | aplicar `fn_pais_id` en los diez `select_silver_*` (hoy comparan por prefijo con `LIMIT 1`); casos sin resolver: `EUROPE`, `EN`, `EL` (45 de fb), 6 ciudades de ml, `UNITED STATES MINOR` (22 de vc) |
| `ingreso` y `egreso` | siguen en los selects de Silver (fijos en 0); decidido quitarlos |
| Revivir clientes | el CDC y el UPDATE de Silver no limpian `deletedAt`; en fi hay 1,271 clientes marcados que existen hoy y no están en Silver (marcados en bloque el 2026-09-24; cumplen el criterio del SP de borrado). Decisión 2026-11-18: no se modifica el código; se monitorea (el algoritmo actual cubre a los clientes con correo real, que es lo que llega a Gold). De los 1,271 marcados solo 1 tiene correo real: `client_id` 59905 (anotado). Hoy no entraría a Silver porque el INSERT solo toma ids mayores que el máximo; si se quisiera ver, habría que quitarle el `deletedAt` y revisar `source.clientsfi` y `gral.factClientesDetalle` (clientid 599053). Se probó un cambio (INSERT por etapas con NOT EXISTS) y se descartó; el borrador quedó archivado en `scripts/db_utils/archivo/`. Monitoreo: cada quincena, los clientes con correo real vivos en Silver y ausentes de `factClientesDetalle` deben ser 0 |
| Teléfonos sin correo | 5,435 teléfonos de clientes sin correo real no pueden entrar a `factPersonalInfo` (decisión de diseño); además 327 teléfonos de fi cuyo cliente no está en Silver: 237 por los clientes marcados (solo 1 de los 1,271 tiene correo real, así que revivirlos no los llevaría a Gold), 87 de clientes que no califican para RAW de clientes y 3 de diferencia de momento |
| Ciclo de borrar y revivir de RAW a Silver | falta probarlo de punta a punta con teléfonos de fb |
| Auditoría de conteo RAW → Silver → Gold | siguiente etapa: comparar cantidades por producto en cada capa |

---

## 13. Validación de la primera corrida automática (domingo 2026-11-15)

Leido de los reportes en `logs/` y de `dw_etl.dw_etl_audit_log`. Los interruptores de Gold estaban encendidos.

| Hora | Pieza | Resultado |
|---|---|---|
| 5:00 | RAW de teléfonos (242 y 240) | OK |
| 6:00 | Silver de clientes (5 en paralelo) | `UPDATE` 1,964 · `INSERT` 1,349 |
| 6:00 | Gold de clientes (`sp_etl_maestro`, 11 s) | `factClientes` +813 / 75 actualizados · detalles insertados 993 + 24 + 281 + 48 + 0 = 1,346 |
| 7:00 | Silver de teléfonos (cadena fb → bb → ml → fi → vc) | `UPDATE` 62 · `INSERT` 685 |
| 7:00 | Gold de teléfonos (`sp_insert_phones_factPersonalInfo`) | insertados 1,186 · revividos 0 · retirados 69 · vivos 451,728 |
| 8:00 y 18:00 | RAW de clientes (242 y 240) | OK |

Comprobaciones contra las líneas base del sábado:

| Tabla | Antes | Después | Diferencia |
|---|---|---|---|
| `factClientes` | 293,509 | 294,322 | +813 |
| `factClientesDetalle` | 510,567 | 511,913 | +1,346 |
| `factPersonalInfo` vivos | 450,611 | 451,728 | +1,117 |

Lectura:

- Las inserciones de cada detalle de Gold igualan a las de Silver (fb 993, bb 24, fi 281); en vc, 48 de 51:
  los 3 restantes son clientes sin correo real.
- Gold de teléfonos insertó más (1,186) que Silver ese día (685): los clientes que llegaron a Gold el domingo
  ya tenían sus teléfonos en Silver y entraron al mapearse.
- Los 69 retiros son el 0.015% de los vivos; la compuerta es del 2%.
- Teléfonos de clientes con correo real que no están en Gold: 0.

Próxima corrida programada: 2026-12-01, 6:00 (clientes) y 7:00 (teléfonos).
