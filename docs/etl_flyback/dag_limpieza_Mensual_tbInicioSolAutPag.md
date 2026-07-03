# dag_limpieza_Mensual_tbInicioSolAutPag

> **Ruta DAG:** `C:\Users\GUSA CAPITAL\Documents\DockersETL\dags\etl_flyback\dag_limpieza_Mensual_tbInicioSolAutPag.py`
> **Módulo / sistema:** flybackDW — SmartData Redeems
> **Responsable:** Andrés José Sarria Correa
> **Última actualización:** 2026-07-03
> **Versión documento:** v1.0

---

## 1. Propósito

Ejecutar una limpieza mensual de registros huérfanos en las tres tablas de inicio de Redeems en `flybackDW`. Un registro huérfano es aquel que existe en el DW pero ya no tiene correspondencia en el origen (`customers`) — por ejemplo, un cliente eliminado o un redeem cancelado. Se ejecuta el día 2 de cada mes a la 1:00am, un día después de que `flybackDW_spInsertHistoricoCobranza` procesó el mes anterior.

---

## 2. Actores

| Rol | Acción que realiza |
|---|---|
| Scheduler Airflow | Dispara el DAG automáticamente el día 2 de cada mes a la 1:00am |
| Data Engineer (Andrés) | Monitorea via email mensual y log .txt |

---

## 3. Caso de uso principal

**Precondición:** Las tres tablas de inicio (`tblInicioSolicitados`, `tblInicioAutorizados`, `tblInicioPagados`) pueden contener registros que ya no existen en el origen.

**Flujo:**
1. El día 2 de cada mes a la 1:00am Airflow dispara el DAG.
2. Tarea 1 — `ejecutar_sp_limpieza`: ejecuta `CALL flybackDW.sp_limpieza_Mensual_tbInicioSolAutPag()`.
3. Tarea 2 — `generar_log_y_notificar`: escribe log .txt y envía email de confirmación.

**Postcondición:** Las tres tablas de inicio no contienen registros huérfanos — el DW está en sincronía con el origen.

---

## 4. Configuración del DAG

| Parámetro | Valor |
|---|---|
| `dag_id` | `dag_limpieza_Mensual_tbInicioSolAutPag` |
| `schedule_interval` | `0 1 2 * *` — día 2 de cada mes 1:00am Cancún |
| `start_date` | `pendulum.datetime(2026, 6, 26, tz="America/Cancun")` |
| `catchup` | `False` |
| `operator` | `PythonOperator` con `MySqlHook` |
| `mysql_conn_id` | `MariaDB` |
| `SP ejecutado` | `flybackDW.sp_limpieza_Mensual_tbInicioSolAutPag()` |

---

## 5. Reglas de negocio

**RN-01: Ejecución el día 2 — después del cierre del mes**
- Se ejecuta el día 2 de cada mes, un día después de que `flybackDW_spInsertHistoricoCobranza` procesó el mes anterior (día 1). Esto garantiza que la limpieza ocurre sobre datos ya consolidados.

**RN-02: Limpieza de tres tablas en un solo SP**
- El SP `sp_limpieza_Mensual_tbInicioSolAutPag` limpia las tres tablas en una sola ejecución transaccional — Solicitados, Autorizados y Pagados.

**RN-03: Timezone explícito**
- Usa `pendulum` con `tz="America/Cancun"` para evitar problemas de horario de verano en el scheduling mensual.

---

## 6. Casos alternos / manejo de errores

| Situación | Comportamiento esperado |
|---|---|
| Error en el SP | `except` captura la excepción — Airflow marca la tarea como `failed` y envía alerta |
| Sin huérfanos que limpiar | SP ejecuta sin cambios — email y log reportan OK |

---

## 7. Dependencias técnicas

**SP:** `flybackDW.sp_limpieza_Mensual_tbInicioSolAutPag()`

**Tablas afectadas:** `flybackDW.tblInicioSolicitados`, `flybackDW.tblInicioAutorizados`, `flybackDW.tblInicioPagados`

**DAG relacionado:** `dag_tbInicioSolAutPag_diario` — mismo conjunto de tablas, distinto propósito (upsert diario vs limpieza mensual)

**Auditoría:** Log .txt en `logs/` y email via SMTP.

---

## 8. Historial de cambios

| Fecha | Versión | Cambio |
|---|---|---|
| 2026-06-19 | v1.1 | Fix timezone — pendulum en lugar de datetime para schedule mensual |
| 2026-07-03 | — | Documentación inicial |
