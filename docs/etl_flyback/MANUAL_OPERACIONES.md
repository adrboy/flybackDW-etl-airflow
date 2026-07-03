# Manual de Operaciones — etl_flyback
> **Ruta:** `C:\Users\GUSA CAPITAL\Documents\DockersETL\docs\etl_flyback\MANUAL_OPERACIONES.md`
> **Responsable:** Andrés José Sarria Correa
> **Última actualización:** 2026-07-03
> **Versión:** v1.0

---

## ¿Para qué sirve este manual?

Este documento describe el procedimiento de verificación diaria/mensual para los DAGs del módulo `etl_flyback`. Cualquier persona del equipo puede seguir estos pasos para confirmar que los procesos corrieron correctamente, en el orden correcto.

---

## 1. Orden de verificación (SIEMPRE este orden)

```
1. etl_audit_log  (MariaDB flybackDW)   ← primera fuente de verdad
2. dag_run        (PostgreSQL Airflow)   ← confirma que Airflow lo disparó
3. Correo         (Email notificación)   ← confirmación visual rápida
4. Log .txt       (Carpeta logs/)        ← detalle completo del run
```

> ⚠️ **Importante:** Si `etl_audit_log` dice `OK` pero el correo no llegó — el proceso corrió bien. El correo es informativo, no es la fuente de verdad.

---

## 2. Verificación en etl_audit_log (MariaDB)

Conectarse a **MariaDB 242** → base de datos **flybackDW** y ejecutar:

### Ver últimas ejecuciones de todos los DAGs
```sql
SELECT paquete, max_id_inicio, filas_insertadas
     , tipo_ejecucion, estado, fecha_inicio, fecha_fin
     , mensaje_error
FROM flybackDW.etl_audit_log
ORDER BY fecha_inicio DESC
LIMIT 20;
```

### Ver solo errores
```sql
SELECT *
FROM flybackDW.etl_audit_log
WHERE estado = 'ERROR'
ORDER BY fecha_inicio DESC
LIMIT 10;
```

### Verificar un DAG específico
```sql
SELECT paquete, max_id_inicio, filas_insertadas
     , estado, fecha_inicio, fecha_fin
FROM flybackDW.etl_audit_log
WHERE paquete = 'spInsertHistoricoCobranza'  -- ← cambiar por el paquete que buscas
ORDER BY fecha_inicio DESC
LIMIT 5;
```

### Estados posibles

| Estado | Significado |
|---|---|
| `RUNNING` | El SP está ejecutándose en este momento |
| `OK` | Completó exitosamente |
| `ERROR` | Falló — revisar columna `mensaje_error` |

---

## 3. Verificación en dag_run (PostgreSQL Airflow)

Conectarse a **PostgreSQL Airflow** → Host: `localhost` Puerto: `5433` → base de datos **airflow** y ejecutar:

### Ver últimos runs de todos los DAGs
```sql
SELECT dag_id, state, queued_at, start_date, end_date
FROM dag_run
ORDER BY queued_at DESC
LIMIT 20;
```

### Ver runs de un DAG específico
```sql
SELECT dag_id, state, queued_at, start_date, end_date
FROM dag_run
WHERE dag_id = 'flybackDW_spInsertHistoricoCobranza'  -- ← cambiar dag_id
ORDER BY queued_at DESC
LIMIT 5;
```

### Ver solo los que fallaron
```sql
SELECT dag_id, state, queued_at, start_date
FROM dag_run
WHERE state = 'failed'
ORDER BY queued_at DESC;
```

### Estados posibles

| Estado | Significado |
|---|---|
| `success` | Completó exitosamente |
| `failed` | Falló — revisar logs de Airflow |
| `running` | Está ejecutándose ahora |
| `queued` | En cola, esperando worker |

---

## 4. Verificación en correo electrónico

El correo llega automáticamente cuando el DAG completa. Buscar en el inbox:

**Asunto:** `ETL Notification — Gusacapital`

**Ejemplo de correo exitoso:**
```
ETL Notification — Gusacapital
==================================================
Proceso  : flybackDW_spInsertHistoricoCobranza
Estado   : OK
Fecha    : 2026-07-01 06:00
==================================================
Contenido del Log:
--------------------------------------------------
[2026-07-01 06:00:XX] DAG: flybackDW_spInsertHistoricoCobranza — INICIO
spInsertHistoricoCobranza(NULL) — OK
Tabla: flybackDW.tbl_historico_cobranza
DAG: flybackDW_spInsertHistoricoCobranza — FIN ✅
```

> ℹ️ Si no llegó el correo pero `etl_audit_log` dice `OK` — el proceso corrió bien. El SMTP puede tener delay o el correo puede estar en spam.

---

## 5. Verificación en Log .txt

Los logs .txt se guardan en:
```
C:\Users\GUSA CAPITAL\Documents\DockersETL\logs\
```

### Buscar logs recientes desde PowerShell
```powershell
# Ver todos los logs de hoy
Get-ChildItem "C:\Users\GUSA CAPITAL\Documents\DockersETL\logs" -Recurse -Filter "*.txt" | 
    Where-Object { $_.LastWriteTime -gt (Get-Date).Date } |
    Sort-Object LastWriteTime -Descending

# Buscar log de un DAG específico
Get-ChildItem "C:\Users\GUSA CAPITAL\Documents\DockersETL\logs" -Recurse -Filter "*spInsertHistorico*" |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 3
```

### Formato del nombre de archivo
```
etl_{nombre_proceso}_FB_log_{YYYYMMDDHHMMSS}.txt
```

Ejemplo:
```
etl_spInsertHistoricoCobranza_FB_log_20260701060038.txt
```

---

## 6. Auditoría de sincronización por mes

### Verificar sincronización de tbl_historico_cobranza

Cambiar el `BETWEEN` según el rango de meses a auditar:

```sql
-- Archivo: C:\Users\GUSA CAPITAL\Documents\mariadb_sql\flybackDW\cobranza\AuditoriaHistoricoCobranza.sql
WITH origen AS (
    SELECT EXTRACT(YEAR_MONTH FROM CC.fcobrado) AS ym
         , CC.indice
         , 1 AS is_packed
         , CC.monto AS monto
    FROM customers.cardcollec CC
    LEFT JOIN customers.fb_clients C ON C.clientid = CC.clientid
    WHERE CC.fcobrado IS NOT NULL
    AND CC.statusc IN (1,2,3,6,7,9,10,12)
    AND C.dppaidin NOT IN (2)
    AND EXTRACT(YEAR_MONTH FROM CC.fcobrado) BETWEEN 202601 AND 202606  -- ← cambiar rango

    UNION ALL

    SELECT EXTRACT(YEAR_MONTH FROM pt.reference_date) AS ym
         , cpp.id AS indice
         , 2 AS is_packed
         , cpp.abono AS monto
    FROM customers.pk_ticket pt
    INNER JOIN customers.carterapagopackeados cpp ON cpp.ticket_id = pt.idticket AND cpp.tipodoc IN ('NC','PA','DTO')
    INNER JOIN customers.cardcollec cc ON cc.indice = cpp.idcc
    INNER JOIN customers.fb_clients fb ON fb.clientid = cc.clientid
    WHERE fb.dppaidin = 2
    AND EXTRACT(YEAR_MONTH FROM pt.reference_date) BETWEEN 202601 AND 202606  -- ← cambiar rango
)
SELECT
     o.ym                                                        AS pay_year_month
   , COUNT(o.indice)                                             AS cnt_origen
   , ROUND(SUM(o.monto), 2)                                      AS monto_origen
   , COUNT(D.indice)                                             AS cnt_destino
   , ROUND(SUM(D.amount_BRUTA_usd), 2)                           AS monto_destino
   , COUNT(o.indice) - COUNT(D.indice)                           AS diff_cnt
   , ROUND(SUM(o.monto) - SUM(D.amount_BRUTA_usd), 2)            AS diff_monto
   , COUNT(D.idcc)                                               AS cnt_idcc
   , COUNT(DISTINCT D.clientid)                                  AS cnt_clientid_uniq
   , COUNT(D.agente)                                             AS cnt_agente
   , COUNT(D.pay_method)                                         AS cnt_pay_method
   , SUM(CASE WHEN D.ModoIN IS NOT NULL THEN 1 ELSE 0 END)       AS cnt_modoin
FROM origen o
LEFT JOIN flybackDW.tbl_historico_cobranza D
       ON D.indice    = o.indice
      AND D.is_packed = o.is_packed
GROUP BY o.ym
ORDER BY o.ym;
```

### Resultado esperado (todo OK)

| pay_year_month | diff_cnt | diff_monto | cnt_idcc | cnt_agente | cnt_pay_method |
|---|---|---|---|---|---|
| 202601 | **0** | **0.00** | = cnt_destino | = cnt_destino | = cnt_destino |
| 202602 | **0** | **0.00** | = cnt_destino | = cnt_destino | = cnt_destino |

> ✅ `diff_cnt = 0` y `diff_monto = 0.00` en todos los meses = sincronización perfecta.
> ⚠️ Si hay diferencias — ejecutar `CALL flybackDW.spInsertHistoricoCobranza(YYYYMM)` para el mes afectado.

### Verificar nulos en columnas clave
```sql
SELECT 
     COUNT(*)                                                AS total_registros
   , SUM(CASE WHEN idcc       IS NULL THEN 1 ELSE 0 END)    AS null_idcc
   , SUM(CASE WHEN clientid   IS NULL THEN 1 ELSE 0 END)    AS null_clientid
   , SUM(CASE WHEN pay_date   IS NULL THEN 1 ELSE 0 END)    AS null_pay_date
   , SUM(CASE WHEN ModoIN     IS NULL THEN 1 ELSE 0 END)    AS null_ModoIN
   , SUM(CASE WHEN agente     IS NULL THEN 1 ELSE 0 END)    AS null_agente
   , SUM(CASE WHEN pay_method IS NULL THEN 1 ELSE 0 END)    AS null_pay_method
   , SUM(CASE WHEN statusc    IS NULL THEN 1 ELSE 0 END)    AS null_statusc
FROM flybackDW.tbl_historico_cobranza;
```

> ✅ Todas las columnas deben dar **0** nulos. `UpdateAt` puede ser NULL — es normal.

---

## 7. Tabla de DAGs y schedules

| DAG | Schedule | Descripción | Verificar |
|---|---|---|---|
| `dag_tbInicioSolAutPag_diario` | L-V 5:30am | Actualiza tblInicio Solicitados/Autorizados/Pagados | Diario |
| `dag_inicio_r_diario` | L-V 8:00am | Actualiza inicio_r en customers.redeems | Diario |
| `flybackDW_spInsertHistoricoCobranza` | Día 1 mes 6am | Inserta mes cerrado en tbl_historico_cobranza | Mensual |
| `dag_limpieza_Mensual_tbInicioSolAutPag` | Día 2 mes 1am | Limpieza de huérfanos en tablas Inicio | Mensual |
| `flybackDW_sp_ActivosRedeemCorp` | Cada lunes 6am | Recarga tblActivosRedeemCorp | Semanal |
| `flybackDW_sp_FinalizarContratosVencidos` | 1 febrero 6am | Finaliza contratos vencidos | Anual |

---

## 8. ¿Qué hacer si algo falla?

### Paso 1 — Identificar el error
```sql
SELECT paquete, estado, mensaje_error, fecha_inicio
FROM flybackDW.etl_audit_log
WHERE estado = 'ERROR'
ORDER BY fecha_inicio DESC
LIMIT 5;
```

### Paso 2 — Ver logs detallados en Airflow
```powershell
docker exec -it airflow_scheduler airflow dags list-runs -d <dag_id>
```

### Paso 3 — Re-ejecutar manualmente
```powershell
docker exec -it airflow_scheduler airflow dags trigger <dag_id>
```

### Paso 4 — Para spInsertHistoricoCobranza — re-ejecutar mes específico
```sql
CALL flybackDW.spInsertHistoricoCobranza(YYYYMM);  -- ej. 202606
```

---

*Documento generado 2026-07-03 | Andrés + CC — Gusacapital*
