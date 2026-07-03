# flybackDW_sp_ActivosRedeemCorp

> **Ruta DAG:** `C:\Users\GUSA CAPITAL\Documents\DockersETL\dags\etl_flyback\flybackDW_sp_ActivosRedeemCorp.py`
> **Módulo / sistema:** flybackDW — SmartData Activos
> **Responsable:** Andrés José Sarria Correa
> **Última actualización:** 2026-07-03
> **Versión documento:** v1.0

---

## 1. Propósito

Recargar semanalmente la tabla `flybackDW.tblActivosRedeemCorp` con el universo de clientes activos con historia de redeems, ejecutando el SP `flybackDW.sp_ActivosRedeemCorp()` cada lunes a las 6:00am. La tabla consolida los activos por corporativo, excluyendo clientes huérfanos mediante validación de `redeem_no = 1`.

---

## 2. Actores

| Rol | Acción que realiza |
|---|---|
| Scheduler Airflow | Dispara el DAG automáticamente cada lunes a las 6:00am |
| Data Engineer (Andrés) | Monitorea via dag_run en Airflow UI |
| Analista / Gerencia | Consume los datos via reportes VB.NET que leen `tblActivosRedeemCorp` |

---

## 3. Caso de uso principal

**Precondición:** Existen registros activos en `customers.redeems` y `customers.fb_clients` con historia de redeems válida.

**Flujo:**
1. Cada lunes a las 6:00am Airflow dispara el DAG.
2. Tarea 1 — `sp_ActivosRedeemCorp`: ejecuta `CALL flybackDW.sp_ActivosRedeemCorp()`.

**Postcondición:** `tblActivosRedeemCorp` contiene el universo actualizado de clientes activos con historia de redeems por corporativo.

---

## 4. Configuración del DAG

| Parámetro | Valor |
|---|---|
| `dag_id` | `flybackDW_sp_ActivosRedeemCorp` |
| `schedule_interval` | `0 6 * * 1` — cada lunes 6:00am Cancún |
| `start_date` | `datetime(2026, 6, 26)` |
| `catchup` | `False` |
| `operator` | `MySqlOperator` |
| `mysql_conn_id` | `MariaDB` |
| `SP ejecutado` | `flybackDW.sp_ActivosRedeemCorp()` |

---

## 5. Reglas de negocio

**RN-01: Exclusión de huérfanos via redeem_no = 1**
- Solo se incluyen clientes que tienen al menos un redeem con `redeem_no = 1` — esto garantiza que el cliente tiene historia real de redeems y no es un registro huérfano o incompleto.

**RN-02: Recarga completa semanal**
- El SP hace recarga completa (TRUNCATE + INSERT) — no es incremental. La frecuencia semanal es suficiente ya que los activos no cambian diariamente de forma significativa.

---

## 6. Casos alternos / manejo de errores

| Situación | Comportamiento esperado |
|---|---|
| Error en el SP | `MySqlOperator` lanza excepción — Airflow marca la tarea como `failed` |
| Sin activos | SP ejecuta con 0 registros — comportamiento anormal que debe investigarse |

> ✅ **v2.0 — 2026-07-03:** Migrado de `MySqlOperator` a `PythonOperator` con email + log .txt.

---

## 7. Dependencias técnicas

**SP:** `flybackDW.sp_ActivosRedeemCorp()`

**Tablas origen:** `customers.redeems`, `customers.fb_clients`, `customers.develops`

**Tabla destino:** `flybackDW.tblActivosRedeemCorp`

**Tabla relacionada:** `flybackDW.tbl_historico_cobranza` — comparten el universo de activos

---

## 8. Historial de cambios

| Fecha | Versión | Cambio |
|---|---|---|
| 2026-05-01 | v1.0 | Implementación inicial |
| 2026-07-03 | — | Documentación inicial |
