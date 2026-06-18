# Doc_limpieza_tbInicioSolAutPag

> **Proyecto:** flybackDW — Migración CTEs onpremise → DW  
> **Dominio:** Redeems Corporativo  
> **Tipo:** ETL Limpieza Mensual  
> **Versión:** 1.0 — 2026-06-18  
> **Autor:** Andrés  

---

## 📋 Descripción General

Este módulo limpia mensualmente los registros huérfanos de las 3 tablas DW de Redeems. Corre el **día 2 de cada mes a la 1:00am hora Cancún** — un día después del 1ro para evitar que los sistemas estén fuera de línea en días festivos.

---

## 🎯 ¿Por qué es necesario?

El sincronizador diario captura cambios via `updateAt`. Sin embargo, soporte técnico puede revertir registros en onpremise **sin que `updateAt` cambie**, dejando el DW con registros que ya no son válidos en la fuente:

| Escenario | Resultado en DW sin limpieza |
|---|---|
| `status_r` revertido a 0 | Registro inválido permanece en DW |
| `fCorreo` anulado | Redeem fantasma en DW |
| `status_p` revertido a 0 | Autorización/pago inválido en DW |
| `pagoid` desvinculado | Huérfano sin redeem asociado |

---

## 🗑️ Condiciones de Limpieza

### tblInicioSolicitados
| Condición | Descripción |
|---|---|
| `IFNULL(B.status_r, 0) = 0` | Nunca completado o revertido |
| `ISNULL(B.fCorreo)` | Fecha de solicitud anulada |
| `t.fecha < año actual` | Solo histórico — no tocar año en curso |

### tblInicioAutorizados
| Condición | Descripción |
|---|---|
| `IFNULL(A.status_p, 0) = 0` | Autorización revertida |
| `redeems.pagoid` no existe | Pago huérfano sin redeem asociado |
| `t.fecha < año actual` | Solo histórico |

### tblInicioPagados
| Condición | Descripción |
|---|---|
| `IFNULL(A.status_p, 0) = 0` | Pago revertido |
| `redeems.pagoid` no existe | Pago huérfano sin redeem asociado |
| `t.fecha < año actual` | Solo histórico |

> **Nota:** El año en curso **nunca se toca** — puede haber reversiones activas que aún se van a resolver.

---

## ⚙️ Stored Procedure

```
flybackDW.sp_limpieza_Mensual_tbInicioSolAutPag
```

### Flujo interno
```
INSERT etl_audit_log (RUNNING)
        ↓
DELETE tblInicioSolicitados  (status_r=0 o fCorreo NULL)
        ↓
DELETE tblInicioAutorizados  (status_p=0)
DELETE tblInicioAutorizados  (huérfanos sin pagoid)
        ↓
DELETE tblInicioPagados      (status_p=0)
DELETE tblInicioPagados      (huérfanos sin pagoid)
        ↓
UPDATE etl_audit_log (OK + filas_afectadas)
```

### Log de auditoría
Registra en `flybackDW.etl_audit_log`:

| Campo | Valor |
|---|---|
| `paquete` | `sp_limpieza_Mensual_tbInicioSolAutPag` |
| `tipo_ejecucion` | `MENSUAL` |
| `estado` | `RUNNING` → `OK` / `ERROR` |
| `filas_insertadas` | Total borrado SOL + AUT + PAG |
| `mensaje_error` | `SOL: N \| AUT: N \| PAG: N` |

### Consultar último resultado
```sql
SELECT *
FROM   flybackDW.etl_audit_log
WHERE  paquete = 'sp_limpieza_Mensual_tbInicioSolAutPag'
ORDER BY id DESC
LIMIT 5;
```

---

## 🔄 DAG Airflow

```
dag_limpieza_Mensual_tbInicioSolAutPag
```

| Parámetro | Valor |
|---|---|
| Schedule | `0 6 2 * *` |
| Hora Cancún | 1:00am día 2 de cada mes |
| Timezone | `America/Cancun` |
| Archivo | `dags/etl_flyback/dag_limpieza_Mensual_tbInicioSolAutPag.py` |
| catchup | False |
| Conexión | `MariaDB` |

### Flujo de tareas
```
ejecutar_sp_limpieza
        ↓
generar_log_y_notificar
```

---

## 🔍 Auditoría manual

Antes de ejecutar el SP manualmente verificar cuántos registros serán afectados:

```sql
-- Verificar huérfanos actuales
SELECT 'Solicitados' AS tabla, COUNT(*) AS huerfanos
FROM   flybackDW.tblInicioSolicitados t
INNER JOIN customers.redeems B ON B.indice = t.contador
WHERE  (IFNULL(B.status_r, 0) = 0 OR ISNULL(B.fCorreo))
AND    t.fecha < DATE_FORMAT(NOW(), '%Y-01-01')

UNION ALL

SELECT 'Autorizados_status0', COUNT(*)
FROM   flybackDW.tblInicioAutorizados t
INNER JOIN customers.pago_redeem A ON A.indice = t.contador
WHERE  IFNULL(A.status_p, 0) = 0
AND    t.fecha < DATE_FORMAT(NOW(), '%Y-01-01')

UNION ALL

SELECT 'Autorizados_huerfanos', COUNT(*)
FROM   flybackDW.tblInicioAutorizados t
LEFT JOIN customers.redeems B ON B.pagoid = t.contador
WHERE  B.indice IS NULL
AND    t.fecha < DATE_FORMAT(NOW(), '%Y-01-01')

UNION ALL

SELECT 'Pagados_status0', COUNT(*)
FROM   flybackDW.tblInicioPagados t
INNER JOIN customers.pago_redeem A ON A.indice = t.pagoid
WHERE  IFNULL(A.status_p, 0) = 0
AND    t.fecha < DATE_FORMAT(NOW(), '%Y-01-01')

UNION ALL

SELECT 'Pagados_huerfanos', COUNT(*)
FROM   flybackDW.tblInicioPagados t
LEFT JOIN customers.redeems B ON B.pagoid = t.pagoid
WHERE  B.indice IS NULL
AND    t.fecha < DATE_FORMAT(NOW(), '%Y-01-01');
```

Si el resultado es 0 en todas — las tablas están limpias.

---

## ⚠️ Consideraciones importantes

- **No borrar año en curso** — registros de 2026 pueden estar en proceso de reversión activa
- **Pagados son los más seguros** — muy raro que un pago se revierta, pero el SP lo cubre por consistencia
- **El sincronizador es la primera defensa** — si `updateAt` cambia, el DAG diario actualiza el DW antes de que llegue la limpieza mensual

---

## 📅 Historial

| Versión | Fecha | Cambio |
|---|---|---|
| 1.0 | 2026-06-18 | Creación inicial |
