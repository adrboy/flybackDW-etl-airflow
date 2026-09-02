# flybackDW_sp_ActivosRedeemCorp

> **Ruta DAG:**  `C:\Users\GUSA CAPITAL\Documents\DockersETL\dags\etl_flyback\flybackDW_sp_ActivosRedeemCorp.py`
> **Ruta SP:**   `C:\Users\GUSA CAPITAL\Documents\mariadb_sql\flybackDW\03 - Redeems\cns_RedeemCorporativo.sql`
> **Ruta SQL Auditoría:** `C:\Users\GUSA CAPITAL\Documents\mariadb_sql\flybackDW\03 - Redeems\AuditoriaActivos.sql`
> **Módulo / sistema:** flybackDW — SmartData Activos Redeem
> **Responsable:** Andrés José Sarria Correa
> **Última actualización:** 2026-09-02
> **Versión documento:** v2.0

---

## 1. Propósito

Recargar semanalmente la tabla `flybackDW.tblActivosRedeemCorp` con el universo completo de clientes activos con historia de redeems, ejecutando el SP `flybackDW.sp_ActivosRedeemCorp()` cada lunes a las 6:00am (Cancún).

La tabla es la **fuente de verdad** para los reportes de activos por corporativo. La consume directamente el SP `flybackDW.cns_RedeemCorporativo(p_anio)` que alimenta el formulario **Reporte de Redeem x Corporativo** en `flybackdash`.

---

## 2. Actores

| Rol | Acción que realiza |
|---|---|
| Scheduler Airflow | Dispara el DAG automáticamente cada lunes a las 6:00am |
| Data Engineer (Andrés) | Monitorea en Airflow UI y ejecuta manualmente si hace falta |
| Analista / Gerencia | Consume los datos via `flybackdash` → Reporte Redeem x Corporativo |

---

## 3. Caso de uso principal

**Precondición:** Existen registros activos en `customers.redeems` y `customers.fb_clients` con historia de redeems válida.

**Flujo:**
1. Cada lunes a las 6:00am Airflow dispara el DAG.
2. Tarea 1 — `sp_ActivosRedeemCorp`: ejecuta `CALL flybackDW.sp_ActivosRedeemCorp()`.
3. El SP hace TRUNCATE + INSERT completo de `tblActivosRedeemCorp`.
4. El SP registra inicio y fin en `flybackDW.tblJobsRegistros`.
5. Tarea 2 — `generar_log_y_notificar`: escribe log .txt y envía email de confirmación.

**Postcondición:** `tblActivosRedeemCorp` contiene el universo actualizado de clientes activos con historia de redeems, listo para ser consumido por `cns_RedeemCorporativo`.

---

## 4. Configuración del DAG

| Parámetro | Valor |
|---|---|
| `dag_id` | `flybackDW_sp_ActivosRedeemCorp` |
| `schedule_interval` | `0 6 * * 1` — cada lunes 6:00am Cancún |
| `start_date` | `datetime(2026, 6, 26)` |
| `catchup` | `False` |
| `operator` | `PythonOperator` |
| `mysql_conn_id` | `MariaDB` |
| `SP ejecutado` | `flybackDW.sp_ActivosRedeemCorp()` |
| `tags` | `flybackDW`, `semanal`, `activos`, `mariadb` |

---

## 5. Estructura de la tabla destino

### `flybackDW.tblActivosRedeemCorp`

| Columna | Tipo | Descripción |
|---|---|---|
| `clientid` | int(10) PK | ID del cliente |
| `Oportunidad` | bigint(21) | Cantidad de redeems históricos anteriores al año actual |
| `idcorp` | int(11) | ID del corporativo al que pertenece |
| `pack` | int(1) | 1 = PACK (`dppaidin = 2`) / 0 = NOPACK |
| `statusf` | decimal(11,2) | Status del cliente en `fb_clients` |
| `inicio_r` | int(10) | Año de inicio del redeem (`YEAR(inicio_r)`) |
| `inicio_rr` | date | Fecha completa de inicio del redeem |

---

## 6. Reglas de negocio

**RN-01: Exclusión de huérfanos via `redeem_no = 1`**
- Solo se incluyen clientes que tienen al menos un redeem con `redeem_no = 1`.
- Esto garantiza que el cliente tiene historia real de redeems y no es un registro huérfano o incompleto.
- Hay 4 clientes con secuencias corruptas que fueron corregidos manualmente (junio 2026).

**RN-02: Recarga completa semanal — TRUNCATE + INSERT**
- El SP hace recarga completa cada lunes — no es incremental.
- La frecuencia semanal es suficiente ya que los activos no cambian diariamente.
- El lunes fue elegido intencionalmente: el Data Engineer está presente para monitorear y actuar si algo falla.

**RN-03: Mapeo de pack**
- En `customers.fb_clients` el campo se llama `dppaidin`.
- El SP transforma: `IF(dppaidin = 2, 1, 0) AS pack` al insertar en la tabla.
- En la tabla materializada `pack` es siempre 0 o 1 — nunca NULL.

**RN-04: Filtro de status activo**
- Solo se incluyen clientes con `status IN (-4)` OR `(status > 3 AND status < 6)`.
- `status > 3 AND status < 6` = activos vigentes.
- `status = -4` = finalizados — se incluyen porque forman parte del histórico acumulado.

**RN-05: Oportunidad — redeems históricos**
- La columna `Oportunidad` cuenta cuántos redeems anteriores al año actual tiene el cliente.
- Fórmula: `COUNT(DISTINCT IF(YEAR(XII.inicio_r) < YEAR(NOW()), XII.redeem_no, NULL))`.
- Representa el "potencial histórico" del cliente para análisis de breakage acumulado.

---

## 7. Auditoría mensual

Para verificar que la tabla está sincronizada con la fuente onpremise, existe una query de auditoría que compara año por año. Si todos los años dan **diferencias = 0** la tabla está correcta.

**Archivo:** `AuditoriaActivos.sql`
**Ruta:** `C:\Users\GUSA CAPITAL\Documents\mariadb_sql\flybackDW\03 - Redeems\AuditoriaActivos.sql`

**Cómo interpretar el resultado:**

| diferencias_PACK | diferencias_NOPACK | Significado |
|---|---|---|
| 0 | 0 | ✅ Año sincronizado |
| > 0 | cualquier valor | ⚠️ Hay clientes en la fuente que no están en la tabla — ejecutar el SP |
| < 0 | cualquier valor | ⚠️ Hay clientes en la tabla que ya no existen en la fuente — ejecutar el SP |

**Cuándo correrla:** una vez al mes, preferiblemente el lunes después de que el DAG ejecutó.

---

## 8. Formulario que consume esta tabla

**Formulario:** `Reporte de Redeem x Corporativo`
**Clase VB.NET:** `ReporteRedeemCorporativo_II`
**Proyecto:** `Dashboard_flyback\flybackdash\03-Redeems\ReporteRedeemCorporativo_II.vb`
**SP intermedio:** `flybackDW.cns_RedeemCorporativo(p_anio INT)`
**Ruta SP:** `C:\Users\GUSA CAPITAL\Documents\mariadb_sql\flybackDW\03 - Redeems\cns_RedeemCorporativo.sql`

El formulario tiene dos vistas:
- **Global** — una fila por corporativo con totales.
- **Pack / NoPack** — misma fila separada en dos columnas: clientes PACK y NO PACK.

En modo **Histórico** (años anteriores al actual) el conteo de activos incluye los finalizados (`statusf = -4`) sumados a los vigentes — esto es correcto por diseño.

---

## 9. Casos alternos / manejo de errores

| Situación | Comportamiento esperado |
|---|---|
| Error en el SP | Se inserta registro de error en `flybackDW.tblJobsRegistros`. Airflow marca la tarea como `failed`. Se envía email de alerta. |
| Sin activos | SP ejecuta con 0 registros — comportamiento anormal, investigar. |
| Tabla desactualizada | Correr `AuditoriaActivos.sql` para confirmar diferencias, luego ejecutar SP manualmente desde Navicat: `CALL flybackDW.sp_ActivosRedeemCorp();` |

---

## 10. Dependencias técnicas

**Tablas origen (lectura):**
- `customers.fb_clients` — status, dppaidin, company, inicio_r
- `customers.activos` — valida que el cliente esté en activos
- `customers.redeems` — historia de redeems (`redeem_no > 0` y `redeem_no = 1`)
- `customers.develops` — para obtener `idcorp` desde `company`

**Tabla destino (escritura):**
- `flybackDW.tblActivosRedeemCorp`

**Tabla de auditoría:**
- `flybackDW.tblJobsRegistros` — registra inicio, fin y estado de cada ejecución

**SP consumidor:**
- `flybackDW.cns_RedeemCorporativo(p_anio INT)` — lee `tblActivosRedeemCorp` para el reporte

---

## 11. Historial de cambios

| Fecha | Versión | Cambio |
|---|---|---|
| 2026-05-01 | v1.0 DAG | Implementación inicial con `MySqlOperator` |
| 2026-07-03 | v2.0 DAG | Migrado a `PythonOperator` con `email_notifier` + `audit_logger`. Movido a `dags/etl_flyback/` |
| 2026-09-01 | v1.0 SP `cns_RedeemCorporativo` | Eliminado `CROSS JOIN flybackDW.tbwpack XI` — causaba `ACTIVOSPACK = 0` y `ACTIVOSNOPACK = 0` en el formulario aunque la tabla tuviera datos correctos |
| 2026-09-01 | v1.0 VB.NET `ViewPackBn` | Corregida lógica modo Histórico: cambiado `r.ActivosPackII` → `r.ActivosPack + r.ActivosPackII` para incluir activos vigentes + finalizados |
| 2026-09-02 | v2.0 Doc | Documentación actualizada con auditoría, fix del SP, fix VB.NET y referencias cruzadas |
