# flybackDW_spInsertHistoricoCobranza

> **Ruta DAG:** `C:\Users\GUSA CAPITAL\Documents\DockersETL\dags\etl_flyback\flybackDW_spInsertHistoricoCobranza.py`
> **Ruta SP:** `C:\Users\GUSA CAPITAL\Documents\mariadb_sql\flybackDW\cobranza\spInsertHistoricoCobranza_V5.sql`
> **Módulo / sistema:** flybackDW — SmartData Cobranza
> **Responsable:** Andrés José Sarria Correa
> **Última actualización:** 2026-07-03
> **Versión documento:** v2.0

---

## 1. Propósito

Automatizar la carga mensual del histórico de cobranza en `flybackDW.tbl_historico_cobranza`, ejecutando el stored procedure `spInsertHistoricoCobranza(NULL)` el primer día de cada mes a las 6:00 AM. La tabla centraliza todos los cobros históricos desde 2013 hasta la fecha, separando dos universos: cobros **NO PACKED** (vía `customers.cardcollec`) y cobros **PACKED** (vía `customers.pk_ticket` + `customers.carterapagopackeados`), con clave compuesta `(indice, is_packed)` para evitar colisiones entre ambos universos que pueden compartir números de índice.

---

## 2. Actores

| Rol | Acción que realiza |
|---|---|
| Scheduler Airflow | Dispara el DAG automáticamente el día 1 de cada mes a las 6:00 AM |
| Data Engineer (Andrés) | Ejecuta manualmente el SP por mes específico para reconstrucción histórica o corrección puntual |
| Analista / Gerencia | Consume los datos via reportes VB.NET (`flybackdash`) que leen `tbl_historico_cobranza` |

---

## 3. Caso de uso principal

**Precondición:** Existen registros de cobros en `customers.cardcollec` (NO PACKED) y en `customers.pk_ticket` + `customers.carterapagopackeados` (PACKED) para el mes a procesar.

**Flujo:**
1. El día 1 de cada mes a las 6:00 AM, Airflow ejecuta el DAG `flybackDW_spInsertHistoricoCobranza`.
2. El DAG llama `CALL flybackDW.spInsertHistoricoCobranza(NULL)`.
3. El SP calcula automáticamente el mes anterior: `EXTRACT(YEAR_MONTH FROM DATE_SUB(NOW(), INTERVAL 1 MONTH))`.
4. El SP registra el inicio en `flybackDW.tblJobsRegistros`.
5. El SP ejecuta un `INSERT ... ON DUPLICATE KEY UPDATE` con dos fuentes via `UNION ALL`:
   - **Rama NO PACKED:** desde `customers.cardcollec` con `dppaidin NOT IN (2)`
   - **Rama PACKED:** desde `customers.pk_ticket` + `customers.carterapagopackeados` con `dppaidin = 2`
6. Adicionalmente, el SP captura registros de **meses ya cerrados** que fueron modificados en el mes que se está procesando, usando `customers.colleclog` como fuente de auditoría (`reg = 'UPDATE'`).
7. El SP registra el cierre con el conteo de registros afectados en `tblJobsRegistros`.
8. Airflow envía notificación por email con el resultado.

**Postcondición:** `tbl_historico_cobranza` contiene todos los cobros del mes anterior, más cualquier corrección de meses históricos detectada vía `colleclog`. Los registros existentes se actualizan via `ON DUPLICATE KEY UPDATE` si alguna columna relevante cambió.

---

## 4. Parámetros y tabla destino

### Parámetro del SP

| Parámetro | Tipo | Descripción |
|---|---|---|
| `p_year_month` | INT | Formato YYYYMM (ej. `202606`). Si es `NULL`, calcula el mes anterior automáticamente. |

### Tabla destino: `flybackDW.tbl_historico_cobranza`

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | int(11) PK | Auto increment |
| `indice` | int(11) | cardcollec.indice (NO PACKED) o carterapagopackeados.id (PACKED) |
| `is_packed` | tinyint(4) | 1 = NO PACKED, 2 = PACKED |
| `idcc` | int(11) | cardcollec.indice — para PACKED es diferente al indice principal |
| `clientid` | int(11) | ID del cliente |
| `pay_year` | int(11) | Año del pago |
| `pay_month` | int(11) | Mes del pago |
| `pay_date` | date | Fecha exacta del pago (`fcobrado` o `reference_date`) |
| `idcorp` | int(11) | ID corporativo |
| `iddev` | varchar(10) | ID desarrolladora |
| `ModoIN` | tinyint(4) | Tipo de cobro (ver RN-02) |
| `agente` | varchar(50) | Agente que procesó el cobro |
| `amount_BRUTA_usd` | decimal(18,6) | Monto bruto en USD (sin conversión — currency guardado para aplicar TC en consulta) |
| `amount_NETA_usd` | decimal(18,6) | Monto neto en USD |
| `currency` | varchar(3) | Moneda original (USD / MXN) |
| `pay_method` | varchar(2) | Método de pago (PA/CC/WT/RA/CR/CH/OT) |
| `statusc` | int(11) | Status del cobro en `cardcollec` |
| `InsertAt` | datetime | Fecha de inserción |
| `UpdateAt` | datetime | Fecha de última actualización (solo cambia si hay diferencia real) |

**Clave compuesta única:** `(indice, is_packed)` — permite que el mismo número de índice exista para NO PACKED y PACKED sin colisión, ya que `cardcollec.indice` y `carterapagopackeados.id` son secuencias independientes que pueden coincidir numéricamente.

---

## 5. Reglas de negocio

**RN-01: Cálculo del mes a procesar**
- **Dado que** el SP puede llamarse con o sin parámetro
- **Cuando** `p_year_month` es `NULL`
- **Entonces** el SP calcula `EXTRACT(YEAR_MONTH FROM DATE_SUB(NOW(), INTERVAL 1 MONTH))` — siempre procesa el mes anterior al momento de ejecución
- **Cuando** `p_year_month` tiene valor (ej. `202604`)
- **Entonces** procesa ese mes específico — útil para reconstrucción histórica o corrección puntual

**RN-02: Clasificación ModoIN**
- **Dado que** cada cobro tiene un tipo según su método de pago
- **Cuando** el SP clasifica el ModoIN
- **Entonces** aplica la siguiente lógica basada en `dppaidin` y `with_card`:

| ModoIN | Descripción | Condición |
|---|---|---|
| 1 | Con Tarjeta | `with_card=1 AND dppaidin IN (1,3,6,7,8,9)` |
| 2 | Pack | `dppaidin=2` (siempre para PACKED) |
| 3 | Sin Tarjeta | `dppaidin IN (4,5) OR clientid < 0` |
| 4 | Tokenizado | `dppaidin IN (10)` |
| 5 | UVC-Link | `dppaidin IN (11)` |
| 0 | Sin clasificar | Cualquier otro caso |

**RN-03: Separación NO PACKED vs PACKED**
- **Dado que** existen dos universos de cobro con estructuras distintas
- **Cuando** el SP procesa los datos
- **Entonces** usa `UNION ALL` con dos ramas separadas:
  - **NO PACKED:** `cardcollec` con `dppaidin NOT IN (2)` — `indice = cardcollec.indice`, `idcc = cardcollec.indice` (iguales)
  - **PACKED:** `pk_ticket + carterapagopackeados` con `dppaidin = 2` — `indice = carterapagopackeados.id`, `idcc = cardcollec.indice` (diferentes)
- **Nota:** la columna `idcc` es crítica para PACKED — permite hacer JOIN de vuelta a `cardcollec` para obtener `clientid`, `agente` y `pay_method`

**RN-04: Montos RAW sin conversión de moneda**
- **Dado que** los cobros pueden estar en USD o MXN
- **Cuando** el SP inserta los montos
- **Entonces** guarda el monto RAW sin aplicar tipo de cambio, y guarda `currency` para que la función `TipoCambioMONEDAS()` se aplique en tiempo de consulta en los reportes
- **Nota:** `amount_NETA_usd` es 0 para cobros con `statusc NOT IN (1,2,3)` (NO PACKED) o `tipodoc != 'PA'` (PACKED)

**RN-05: Captura de modificaciones en meses cerrados vía colleclog**
- **Dado que** el soporte puede modificar registros de meses ya cerrados después del cierre
- **Cuando** el SP ejecuta el mes `p_year_month`
- **Entonces** además del mes normal, captura registros históricos que fueron modificados en ese mismo mes (`EXTRACT(YEAR_MONTH FROM colleclog.fecha) = p_year_month`) pero cuyo `fcobrado` es de un mes anterior (`< p_year_month`)
- **Mecanismo:** usa `ROW_NUMBER() OVER (PARTITION BY cardcollec_id ORDER BY fecha DESC)` para tomar solo el último UPDATE por registro, y compara 5 columnas clave contra destino: `monto`, `statusc`, `fcobrado`, `currency`, `pay_method`
- **Condición de reproceso:** `H.indice IS NULL` (no existe en destino) OR alguna columna difiere

**RN-06: Control de actualización — UpdateAt**
- **Dado que** el SP puede ejecutarse múltiples veces para el mismo mes (idempotente)
- **Cuando** el `ON DUPLICATE KEY UPDATE` detecta una colisión de clave
- **Entonces** actualiza todas las columnas pero solo cambia `UpdateAt` si hay diferencia real en: `amount_BRUTA_usd`, `amount_NETA_usd`, `statusc`, `pay_date`, `clientid`, `idcc`, `ModoIN`, `agente` o `pay_method`
- **Nota:** usa `COALESCE(col, valor_neutro)` en todas las comparaciones para manejar correctamente el caso `NULL <> valor`

---

## 6. Casos alternos / manejo de errores

| Situación | Comportamiento esperado |
|---|---|
| Error SQL durante el INSERT | `EXIT HANDLER FOR SQLEXCEPTION` ejecuta `ROLLBACK` y registra el error en `tblJobsRegistros` con `\| ERROR ROLLBACK` |
| Mes sin datos en origen | El SP inserta 0 registros y registra `con 0 registros \| OK` — sin error |
| Mismo mes ejecutado dos veces | `ON DUPLICATE KEY UPDATE` — segunda ejecución actualiza si hay cambios, si no hay cambios reporta 0 |
| `colleclog` sin registros del mes | El OR simplemente no agrega registros adicionales — comportamiento normal |
| Índice coincidente entre NO PACKED y PACKED | La clave compuesta `(indice, is_packed)` los diferencia correctamente — pueden coexistir sin colisión |

---

## 7. Dependencias técnicas

**DAG Airflow:**
- `dag_id`: `flybackDW_spInsertHistoricoCobranza`
- `schedule_interval`: `0 6 1 * *` — día 1 de cada mes a las 6:00 AM (Cancún UTC-5)
- `start_date`: `datetime(2026, 7, 2)` — actualizado para evitar catchup
- `catchup`: `False`
- `operator`: `MySqlOperator` con `mysql_conn_id = 'MariaDB'`

**Stored Procedure:** `flybackDW.spInsertHistoricoCobranza(p_year_month INT)` — versión activa: V5

**Tablas consultadas (lectura):**
- `customers.cardcollec` — cobros NO PACKED
- `customers.carterapagopackeados` — cobros PACKED (parcialidades)
- `customers.pk_ticket` — tickets PACKED
- `customers.fb_clients` — datos del cliente (`dppaidin`, `with_card`, `company`)
- `customers.develops` — corporativo (`idcorp`, `iddev`)
- `customers.colleclog` — auditoría de UPDATEs para capturar modificaciones históricas

**Tabla destino (escritura):**
- `flybackDW.tbl_historico_cobranza`

**Tabla de auditoría:**
- `flybackDW.tblJobsRegistros` — registra inicio, cierre y conteo de registros por ejecución

**Índices clave en colleclog (creados 2026-07-02):**
- `idx_colleclog_reg_fecha (reg, fecha)` — mejora 75x (32s → 0.4s)
- `idx_colleclog_reg_fecha_fcobrado (reg, fecha, fcobrado)`

**Pendiente (V6):** cuando se agregue columna `updateAt` a `customers.cardcollec` con trigger BEFORE UPDATE, el OR del `colleclog` se simplificará a:
```sql
OR (EXTRACT(YEAR_MONTH FROM CC.updateAt) = p_year_month
    AND EXTRACT(YEAR_MONTH FROM CC.fcobrado) < p_year_month)
```

---

## 8. Historial de cambios

| Fecha | Versión SP | Cambio | Quién |
|---|---|---|---|
| 2026-05-01 | V1 | Implementación inicial — NO PACKED + PACKED básico | Andrés |
| 2026-06-02 | V2 | Eliminado filtro `fcobrado <= 2023-12-31`. Corregida lógica neta PACKED (`tipodoc='PA'`). Reconstrucción histórica 2013-202605 | Andrés + CC |
| 2026-06-29 | V3 | Nuevas columnas: `idcc`, `clientid`, `ModoIN`, `agente`, `pay_method`. COALESCE para NULL. Reconstrucción completa certificada 2013-2026 | Andrés + CC |
| 2026-06-30 | V4 | OR colleclog agregado para capturar modificaciones históricas. ROW_NUMBER para tomar solo el último UPDATE | Andrés + CC |
| 2026-07-02 | V5 | OR colleclog mejorado: 5 columnas comparadas vs destino, `H.indice IS NULL` para nuevos. `pay_year/pay_month/pay_date` actualizables. Variable `p_year_month` en lugar de hardcoded | Andrés + CC |
| 2026-07-03 | — | DAGs flybackDW migrados de `dags/` raíz a `dags/etl_flyback/`. Documentación migrada a `docs/etl_flyback/` | Andrés + CC |
