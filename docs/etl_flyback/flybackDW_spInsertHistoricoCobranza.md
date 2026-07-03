# DAG: flybackDW_spInsertHistoricoCobranza

**Fecha de implementación:** 02 de Junio 2026  
**Proyecto:** flybackDW — Histórico de Cobranza  
**Autor:** Andrés  

---

## 🎯 Objetivo

Automatizar la carga mensual del histórico de cobranza en `flybackDW.tbl_historico_cobranza`, ejecutando el stored procedure `spInsertHistoricoCobranza(NULL)` el primer día de cada mes a las 6:00 AM.

---

## ⚙️ Configuración del DAG

| Parámetro | Valor |
|---|---|
| `dag_id` | `flybackDW_spInsertHistoricoCobranza` |
| `schedule_interval` | `0 6 1 * *` — día 1 de cada mes a las 6 AM |
| `start_date` | 2026-05-01 |
| `catchup` | `True` — ejecuta runs pendientes si estuvo apagado |
| `mysql_conn_id` | `MariaDB` |
| `sql` | `CALL flybackDW.spInsertHistoricoCobranza(NULL)` |

---

## 🗄️ Stored Procedure: spInsertHistoricoCobranza

### Tabla destino
`flybackDW.tbl_historico_cobranza`

### Estructura de la tabla

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | int(11) PK | Auto increment |
| `indice` | int(11) | Índice del pago (cardcollec o pk_ticket) |
| `is_packed` | tinyint(4) | 1 = No Packed, 2 = Packed |
| `pay_year` | int(11) | Año del pago |
| `pay_month` | int(11) | Mes del pago |
| `pay_date` | date | Fecha exacta del pago |
| `idcorp` | int(11) | ID corporativo |
| `iddev` | varchar(10) | ID desarrolladora |
| `amount_BRUTA_usd` | decimal(18,6) | Monto bruto en USD |
| `amount_NETA_usd` | decimal(18,6) | Monto neto en USD |
| `statusc` | int(11) | Status del cobro |
| `InsertAt` | datetime | Fecha de inserción |
| `UpdateAt` | datetime | Fecha de última actualización |

### Parámetro
- `p_year_month INT` — formato YYYYMM (ej. `202605`)
- `NULL` → calcula el mes anterior automáticamente

### Lógica de dos fuentes (UNION ALL)

#### Parte 1 — NO PACKED (`cardcollec`)
```sql
FROM customers.cardcollec       CC
LEFT JOIN customers.fb_clients  C   ON C.clientid = CC.clientid
INNER JOIN customers.develops   dv  ON dv.iddev   = C.company
WHERE CC.fcobrado  IS NOT NULL
  AND CC.statusc   IN (1, 2, 3, 6, 7, 9, 10, 12)
  AND C.dppaidin   NOT IN (2)
  AND EXTRACT(YEAR_MONTH FROM CC.fcobrado) = p_year_month
GROUP BY CC.indice
```

#### Parte 2 — PACKED (`pk_ticket` desde 2024)
```sql
FROM customers.pk_ticket                    pt
INNER JOIN customers.carterapagopackeados   cpp ON cpp.ticket_id = pt.idticket
                                               AND cpp.tipodoc   IN ('NC', 'PA', 'DTO')
INNER JOIN customers.cardcollec             cc  ON cc.indice     = cpp.idcc
INNER JOIN customers.fb_clients             fb  ON fb.clientid   = cc.clientid
INNER JOIN customers.develops               dv  ON dv.iddev      = fb.company
WHERE fb.dppaidin = 2
  AND EXTRACT(YEAR_MONTH FROM pt.reference_date) = p_year_month
GROUP BY cpp.id
```

### Lógica de montos

| Columna | NO PACKED | PACKED |
|---|---|---|
| `amount_BRUTA_usd` | `IF(currency='MXN', monto/TC, monto)` | `IF(currency='MXN', abono/TC, abono)` |
| `amount_NETA_usd` | `IF(statusc IN (1,2,3), bruta, 0)` | `IF(tipodoc IN ('PA'), bruta, 0)` |

### Control de duplicados
```sql
ON DUPLICATE KEY UPDATE
  amount_BRUTA_usd = VALUES(amount_BRUTA_usd)
, amount_NETA_usd  = VALUES(amount_NETA_usd)
, statusc          = VALUES(statusc)
, UpdateAt = IF(cambio_detectado, NOW(), UpdateAt)
```

### Auditoría
Registra inicio y cierre en `flybackDW.tblJobsRegistros`:
```
Insertando histórico cobranza período: 202605 con 4532 registros | OK
```

---

## 📊 Consulta asociada: cnsHistoricoCobranza

SP que consume la tabla para el reporte en `flybackdash`:

- **PARTE 1** → `tbl_historico_cobranza` (datos < mes actual)
- **PARTE 2** → Consulta en vivo onpremise (datos >= mes actual)
- **Resultado:** Año | Corporativo | No_Packed | Packed | No_Packed_Neta | Packed_Neta | Total | Total_Neta

---

## 🔄 Historial de cambios

### v2 — 02 Junio 2026
- **NO PACKED:** eliminado filtro `(dppaidin <> 2 OR fcobrado <= '2023-12-31')` → reemplazado por `dppaidin NOT IN (2)`
- **PACKED:** eliminado filtro `cc.fcobrado >= '2024-01-01'` — eje de tiempo es `pt.reference_date`
- **PACKED:** corregida lógica de neta: `tipodoc IN ('NC','DTO')` → `tipodoc IN ('PA')`
- **Tabla:** reconstruida limpia desde 2013 hasta 202605 (149 meses)
- **Backup:** tabla original guardada como `tbl_historico_cobranzabk20260602`

### v1 — Mayo 2026
- Implementación inicial con DAG Airflow
- Lógica base NO PACKED + PACKED

---

## ✅ Auditorías validadas

| Año | Total Bruta | Packed Bruta | Total Neta | Estado |
|---|---|---|---|---|
| 2018 | $17,287,450.95 | $5,177,065.78 | — | ✅ |
| 2023 | $37,968,965.69 | $18,535,106.47 | — | ✅ |
| 2025 | $48,104,420.74 | $39,161,131.51 | $44,684,650.32 | ✅ |

---

## 🗓️ Operación mensual

```
Día 1 de cada mes a las 6:00 AM
    └── DAG dispara spInsertHistoricoCobranza(NULL)
            └── Calcula mes anterior automáticamente
            └── Inserta/actualiza registros en tbl_historico_cobranza
            └── Registra resultado en tblJobsRegistros
```

---

*Documento generado el 02/06/2026*
