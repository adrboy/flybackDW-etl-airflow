# dag_tbInicioSolAutPag_diario

> **Ruta DAG:** `C:\Users\GUSA CAPITAL\Documents\DockersETL\dags\etl_flyback\dag_tbInicioSolAutPag_diario.py`
> **Módulo / sistema:** flybackDW — SmartData Redeems
> **Responsable:** Andrés José Sarria Correa
> **Última actualización:** 2026-07-03
> **Versión documento:** v1.0

---

## 1. Propósito

Actualizar diariamente las tres tablas de inicio de Redeems en `flybackDW`, sincronizando datos desde `customers` hacia el Data Warehouse mediante un patrón upsert incremental. Ejecuta tres SPs en secuencia: Solicitados → Autorizados → Pagados, con espera anti-deadlock entre Autorizados y Pagados.

---

## 2. Actores

| Rol | Acción que realiza |
|---|---|
| Scheduler Airflow | Dispara el DAG automáticamente lunes a viernes a las 5:30am |
| Data Engineer (Andrés) | Monitorea via email diario y log .txt |
| Analista / Gerencia | Consume los datos via reportes VB.NET que leen las tablas de inicio |

---

## 3. Caso de uso principal

**Precondición:** Existen registros en `customers.redeems` y `customers.pago_redeem` con actividad del día anterior.

**Flujo:**
1. Lunes a viernes a las 5:30am Airflow dispara el DAG.
2. Tarea 1 — `actualizar_tblInicioSolicitados`: ejecuta `CALL flybackDW.update_flybackDW_tblInicioSolicitados_VI_hour()`.
3. Tarea 2 — `actualizar_tblInicioAutorizados`: ejecuta `CALL flybackDW.update_flybackDW_tblInicioAutorizados_VI_hour()`.
4. Tarea 3 — `actualizar_tblInicioPagados`: espera 10 segundos (anti-deadlock) y ejecuta `CALL flybackDW.update_flybackDW_tblInicioPagados_VI_hour()`.
5. Tarea 4 — `generar_log_y_notificar`: escribe log .txt y envía email de confirmación.

**Postcondición:** Las tres tablas de inicio están sincronizadas con el origen hasta el momento de ejecución.

---

## 4. Configuración del DAG

| Parámetro | Valor |
|---|---|
| `dag_id` | `dag_tbInicioSolAutPag_diario` |
| `schedule_interval` | `30 5 * * 1-5` — lunes a viernes 5:30am Cancún |
| `start_date` | `datetime(2026, 6, 26)` |
| `catchup` | `False` |
| `operator` | `PythonOperator` con `MySqlHook` |
| `mysql_conn_id` | `MariaDB` |

### Tareas y SPs

| Tarea | SP | Origen | Destino | Sleep |
|---|---|---|---|---|
| `actualizar_tblInicioSolicitados` | `update_flybackDW_tblInicioSolicitados_VI_hour` | `customers.redeems` | `flybackDW.tblInicioSolicitados` | 0s |
| `actualizar_tblInicioAutorizados` | `update_flybackDW_tblInicioAutorizados_VI_hour` | `customers.pago_redeem / redeems` | `flybackDW.tblInicioAutorizados` | 0s |
| `actualizar_tblInicioPagados` | `update_flybackDW_tblInicioPagados_VI_hour` | `customers.pago_redeem / redeems` | `flybackDW.tblInicioPagados` | 10s |

---

## 5. Reglas de negocio

**RN-01: Secuencia obligatoria Solicitados → Autorizados → Pagados**
- Los tres SPs deben ejecutarse en orden — Pagados depende de datos que Autorizados ya actualizó.

**RN-02: Sleep anti-deadlock**
- `tblInicioPagados` espera 10 segundos después de `tblInicioAutorizados` para evitar deadlocks en MariaDB cuando ambos SPs tocan tablas relacionadas simultáneamente.

**RN-03: Upsert incremental**
- Cada SP usa `ON DUPLICATE KEY UPDATE` con criterio `MAX(contador) OR DATE(updateAt) > MAX(Create_At)` — solo procesa registros nuevos o modificados desde la última ejecución.

---

## 6. Casos alternos / manejo de errores

| Situación | Comportamiento esperado |
|---|---|
| Error en cualquier SP | `except` captura, inserta en `etl_audit_log` con `estado='ERROR'`, relanza excepción — Airflow marca la tarea como `failed` |
| Sin datos nuevos | SP ejecuta sin cambios — `0 rows affected` — email y log reportan OK igual |
| Falla de conexión a MariaDB | `MySqlHook` lanza excepción — Airflow reintenta según política de reintentos |

---

## 7. Dependencias técnicas

**SPs:** `flybackDW.update_flybackDW_tblInicioSolicitados_VI_hour`, `update_flybackDW_tblInicioAutorizados_VI_hour`, `update_flybackDW_tblInicioPagados_VI_hour`

**Tablas origen:** `customers.redeems`, `customers.pago_redeem`, `customers.fb_clients`

**Tablas destino:** `flybackDW.tblInicioSolicitados`, `flybackDW.tblInicioAutorizados`, `flybackDW.tblInicioPagados`

**Auditoría:** `flybackDW.etl_audit_log` — registro de errores. Log .txt en `logs/` y email via SMTP.

---

## 8. Historial de cambios

| Fecha | Versión | Cambio |
|---|---|---|
| 2026-05-01 | v1.0 | Implementación inicial |
| 2026-06-25 | v3.0 | Función `ejecutar_sp()` reutilizable, SQL externo, sin SQL embebido |
| 2026-07-03 | — | Documentación inicial |
