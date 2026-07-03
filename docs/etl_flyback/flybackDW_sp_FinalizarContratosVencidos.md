# flybackDW_sp_FinalizarContratosVencidos

> **Ruta DAG:** `C:\Users\GUSA CAPITAL\Documents\DockersETL\dags\etl_flyback\flybackDW_sp_FinalizarContratosVencidos.py`
> **Módulo / sistema:** flybackDW — SmartData Activos
> **Responsable:** Andrés José Sarria Correa
> **Última actualización:** 2026-07-03
> **Versión documento:** v1.0

---

## 1. Propósito

Ejecutar anualmente una "red de seguridad" que finaliza contratos vencidos en `flybackDW`, marcando como finalizados aquellos cuyo último redeem ocurrió en el año anterior o antes. Se ejecuta el 1 de febrero de cada año a las 6:00am — después de que enero ya cerró y el año anterior está completamente consolidado.

---

## 2. Actores

| Rol | Acción que realiza |
|---|---|
| Scheduler Airflow | Dispara el DAG automáticamente el 1 de febrero a las 6:00am |
| Data Engineer (Andrés) | Monitorea via dag_run en Airflow UI |

---

## 3. Caso de uso principal

**Precondición:** Existen contratos en `flybackDW` cuyo último redeem fue en el año anterior o antes y que aún no han sido marcados como finalizados.

**Flujo:**
1. El 1 de febrero a las 6:00am Airflow dispara el DAG.
2. Tarea 1 — `sp_FinalizarContratosVencidos`: ejecuta `CALL flybackDW.sp_FinalizarContratosVencidos()`.

**Postcondición:** Los contratos vencidos quedan marcados como finalizados en `flybackDW`, manteniendo la integridad del universo de activos.

---

## 4. Configuración del DAG

| Parámetro | Valor |
|---|---|
| `dag_id` | `flybackDW_sp_FinalizarContratosVencidos` |
| `schedule_interval` | `0 6 1 2 *` — 1 de febrero a las 6:00am |
| `start_date` | `datetime(2027, 2, 1)` — próxima ejecución feb 2027 |
| `catchup` | `False` — no ejecutar años anteriores |
| `operator` | `MySqlOperator` |
| `mysql_conn_id` | `MariaDB` |
| `SP ejecutado` | `flybackDW.sp_FinalizarContratosVencidos()` |

---

## 5. Reglas de negocio

**RN-01: Red de seguridad anual**
- Este DAG es un proceso correctivo — no debería haber contratos vencidos sin finalizar si los procesos diarios y semanales funcionan correctamente. Sin embargo, se ejecuta como garantía de integridad del universo de activos al inicio de cada año.

**RN-02: Ejecución el 1 de febrero**
- Se ejecuta el 1 de febrero (no el 1 de enero) para garantizar que enero del año nuevo ya cerró completamente y el año anterior está 100% consolidado antes de finalizar contratos.

**RN-03: start_date en el futuro**
- `start_date = datetime(2027, 2, 1)` — Airflow no ejecutará este DAG hasta febrero 2027. Si se necesita ejecutar antes, hacerlo manualmente via trigger.

---

## 6. Casos alternos / manejo de errores

| Situación | Comportamiento esperado |
|---|---|
| Error en el SP | `MySqlOperator` lanza excepción — Airflow marca la tarea como `failed` |
| Sin contratos que finalizar | SP ejecuta con 0 cambios — comportamiento normal si los procesos regulares funcionaron bien |
| Ejecución manual anticipada | `docker exec -it airflow_scheduler airflow dags trigger flybackDW_sp_FinalizarContratosVencidos` |

> ✅ **v2.0 — 2026-07-03:** Migrado de `MySqlOperator` a `PythonOperator` con email + log .txt.

---

## 7. Dependencias técnicas

**SP:** `flybackDW.sp_FinalizarContratosVencidos()`

**Tablas afectadas:** tablas de activos en `flybackDW` (contratos y redeems)

**DAG relacionado:** `flybackDW_sp_ActivosRedeemCorp` — comparten el universo de activos

---

## 8. Historial de cambios

| Fecha | Versión | Cambio |
|---|---|---|
| 2026-05-15 | v1.0 | Implementación inicial |
| 2026-07-03 | — | Documentación inicial |
