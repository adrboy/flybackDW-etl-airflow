# dag_inicio_r_diario

> **Ruta DAG:** `C:\Users\GUSA CAPITAL\Documents\DockersETL\dags\etl_flyback\dag_inicio_r_diario.py`
> **Módulo / sistema:** flybackDW — SmartData Redeems
> **Responsable:** Andrés José Sarria Correa
> **Última actualización:** 2026-07-03
> **Versión documento:** v1.0

---

## 1. Propósito

Actualizar diariamente la columna `inicio_r` en `customers.redeems` ejecutando el SP `customers.diario_update_inicio_r`. Migrado desde el Batch Navicat `Batch_Diario_Inicio_r` que corría manualmente a las 8am — ahora completamente automatizado via Airflow.

---

## 2. Actores

| Rol | Acción que realiza |
|---|---|
| Scheduler Airflow | Dispara el DAG automáticamente lunes a viernes a las 8:00am |
| Data Engineer (Andrés) | Monitorea via email diario — reemplaza revisión manual del Batch Navicat |

---

## 3. Caso de uso principal

**Precondición:** Existen registros en `customers.redeems` con actividad del día anterior que requieren actualización de `inicio_r`.

**Flujo:**
1. Lunes a viernes a las 8:00am Airflow dispara el DAG.
2. Tarea 1 — `actualizar_inicio_r`: ejecuta `CALL customers.diario_update_inicio_r()`.
3. Tarea 2 — `generar_log_y_notificar`: escribe log .txt y envía email de confirmación.

**Postcondición:** La columna `inicio_r` en `customers.redeems` está actualizada con las fechas de inicio correctas.

---

## 4. Configuración del DAG

| Parámetro | Valor |
|---|---|
| `dag_id` | `dag_inicio_r_diario` |
| `schedule_interval` | `0 8 * * 1-5` — lunes a viernes 8:00am Cancún |
| `start_date` | `datetime(2026, 6, 26)` |
| `catchup` | `False` |
| `operator` | `PythonOperator` con `MySqlHook` |
| `mysql_conn_id` | `MariaDB` |
| `SP ejecutado` | `customers.diario_update_inicio_r()` |

---

## 5. Reglas de negocio

**RN-01: Migración desde Batch Navicat**
- Este DAG reemplaza el Batch `Batch_Diario_Inicio_r` que corría manualmente en Navicat — mismo horario (8am), mismo SP, ahora completamente automático y con auditoría.

**RN-02: Solo días hábiles**
- El schedule `0 8 * * 1-5` garantiza que solo corre lunes a viernes — los sábados y domingos no se ejecuta.

---

## 6. Casos alternos / manejo de errores

| Situación | Comportamiento esperado |
|---|---|
| Error en el SP | `except` captura, inserta en `etl_audit_log` con `estado='ERROR'`, relanza excepción |
| Sin datos que actualizar | SP ejecuta sin cambios — email y log reportan OK |

---

## 7. Dependencias técnicas

**SP:** `customers.diario_update_inicio_r()`

**Tabla afectada:** `customers.redeems` (columna `inicio_r`)

**Auditoría:** `flybackDW.etl_audit_log` — registro de errores. Log .txt en `logs/` y email via SMTP.

---

## 8. Historial de cambios

| Fecha | Versión | Cambio |
|---|---|---|
| 2026-06-25 | v1.0 | Creación — migración desde Batch Navicat `Batch_Diario_Inicio_r` |
| 2026-07-03 | — | Documentación inicial |
