# Doc_Sincronizador_tbInicioSolAutPag

> **Proyecto:** flybackDW — Migración CTEs onpremise → DW  
> **Dominio:** Redeems Corporativo  
> **Tipo:** ETL Sincronizador  
> **Versión:** 2.1 — 2026-06-16  
> **Autor:** Andrés  

---

## 📋 Descripción General

Este módulo sincroniza los registros de solicitudes, autorizaciones y pagos de Redeems desde las tablas transaccionales onpremise (`customers`) hacia las tablas DW (`flybackDW`). Corre de lunes a viernes a las **5:30am hora Cancún**, antes del `dag_masterclients` para evitar deadlocks.

---

## 🗂️ Tablas Involucradas

### Fuente (onpremise)
| Tabla | Descripción |
|---|---|
| `customers.redeems` | Solicitudes de redeem |
| `customers.pago_redeem` | Autorizaciones y pagos |
| `customers.fb_clients` | Atributos del cliente (pack, company) |
| `customers.develops` | Mapeo empresa → corporativo |

### Destino (DW)
| Tabla | Descripción | PK |
|---|---|---|
| `flybackDW.tblInicioSolicitados` | Redeems solicitados | `contador` |
| `flybackDW.tblInicioAutorizados` | Redeems autorizados | `contador` |
| `flybackDW.tblInicioPagados` | Redeems pagados | `pagoid` |

---

## 🏗️ Estructura de las Tablas DW

Todas las tablas comparten esta estructura base:

| Columna | Tipo | Descripción |
|---|---|---|
| `tipo` | varchar(11) | 'Solicitados' / 'Autorizados' / 'Pagados' |
| `fecha` | date | Fecha del evento |
| `dia` | int(3) | Día del mes |
| `mes` | int(2) | Mes |
| `anio` | int(4) | Año |
| `AnioMes` | int(6) | YYYYMM |
| `NomMes` | varchar(10) | Nombre del mes en español |
| `contador` / `pagoid` | int(10) | PK — índice onpremise |
| `clientid` | int(11) | ID del cliente |
| `pack` | tinyint(1) | 1=PACK (dppaidin=2), 0=NO PACK |
| `monto` | decimal(11,2) | Monto RAW en moneda original |
| `currency` | varchar(3) | Moneda (USD / MXN) |
| `idcorp` | int(11) | ID corporativo |
| `iddev` | varchar(30) | ID developer |
| `status_r` / `status_p` | decimal | Estado del redeem/pago |
| `nivel` | int(1) | 1=Sol, 2=Aut, 3=Pag |
| `eliminado` | tinyint(1) | Soft delete (0=activo, 1=eliminado) |
| `Create_At` | datetime | Fecha inserción DW |
| `Update_At` | datetime | Última actualización |

> **Nota:** `monto` se guarda en moneda original (RAW). La conversión a USD se aplica en tiempo de lectura en los reportes usando `banks.TipoCambioMONEDAS()`.

---

## ⚙️ Stored Procedures de Inserción

### Lógica de sincronización (igual en los 3 SPs)

El algoritmo dual captura:
1. **Registros nuevos:** `indice > MAX(contador)` — nunca vistos
2. **Registros modificados:** `updateAt > MAX(Create_At)` — actualizados en onpremise

```sql
AND (
    B.indice > (SELECT COALESCE(MAX(contador), 0) FROM flybackDW.tblInicioSolicitados)
    OR
    B.updateAt > (SELECT COALESCE(MAX(Create_At), '2000-01-01') FROM flybackDW.tblInicioSolicitados)
)
```

Esto garantiza que si soporte técnico revierte un `status_r` a 0, el DAG lo detecta en la próxima ejecución y actualiza el DW.

### SP Solicitados
```
flybackDW.update_flybackDW_tblInicioSolicitados_VI_hour
```
- **Fuente:** `customers.redeems` + `customers.fb_clients`
- **Filtro:** `NOT ISNULL(B.fCorreo) AND IFNULL(B.status_r, 0) <> 0`
- **Join cliente:** `LEFT JOIN fb_clients` — captura aunque no tenga match

### SP Autorizados
```
flybackDW.update_flybackDW_tblInicioAutorizados_VI_hour
```
- **Fuente:** `customers.pago_redeem` + `customers.redeems` + `customers.fb_clients`
- **Filtro:** `NOT ISNULL(A.f_authorized)`
- **Join:** `INNER JOIN redeems ON redeems.pagoid = pago_redeem.indice`

### SP Pagados
```
flybackDW.update_flybackDW_tblInicioPagados_VI_hour
```
- **Fuente:** `customers.pago_redeem` + `customers.redeems` + `customers.fb_clients`
- **Filtro:** `A.status_P >= 3 AND NOT ISNULL(IFNULL(A.f_excel, A.f_pago))`
- **Fecha:** `IFNULL(A.f_excel, A.f_pago)` — prioriza fecha excel sobre fecha pago

---

## 🔄 DAG Airflow

```
dag_tbInicioSolAutPag_diario
```

| Parámetro | Valor |
|---|---|
| Schedule | `30 10 * * 1-5` |
| Hora Cancún | 5:30am lunes a viernes |
| Archivo | `dags/etl_flyback/dag_tbInicioSolAutPag_diario.py` |
| catchup | False |
| Conexión | `MariaDB` |

### Flujo de tareas
```
actualizar_tblInicioSolicitados
        ↓
actualizar_tblInicioAutorizados
        ↓
actualizar_tblInicioPagados
        ↓
generar_log_y_notificar
```

### Log de auditoría
Cada SP registra en `flybackDW.etl_audit_log`:

| Campo | Valor |
|---|---|
| `tipo_ejecucion` | `HORA` |
| `estado` | `RUNNING` → `OK` / `ERROR` |
| `filas_insertadas` | Registros insertados/actualizados |
| `max_id_inicio` | MAX(contador) antes de ejecutar |

---

## ⚠️ Comportamiento ante reversiones

Soporte técnico puede revertir `status_p = 0` en `pago_redeem` sin borrar el registro. Esto deja el DW desincronizado porque el SP solo procesa registros donde `updateAt` cambió.

**Solución:** El DAG de limpieza mensual (`dag_limpieza_Mensual_tbInicioSolAutPag`) detecta y elimina estos registros el día 2 de cada mes.

---

## 🐛 Troubleshooting

### Deadlock (Error 1213)
**Causa:** Colisión con `dag_masterclients` que también corre a las 6am y toca `customers.fb_clients`.  
**Solución:** El DAG diario corre a las 5:30am — 30 minutos antes.

### DAG falla con scheduled run atrasado
**Causa:** Computadora bloqueada o apagada durante la ejecución programada.  
**Solución:** Dejar monitor apagado pero **no bloquear sesión Windows**. Ejecutar manualmente:
```powershell
docker-compose exec airflow-scheduler airflow dags trigger dag_tbInicioSolAutPag_diario
```

### Verificar pendientes antes del próximo DAG
```sql
SELECT COUNT(*) AS pendientes_sol
FROM   customers.redeems B
WHERE  NOT ISNULL(B.fCorreo)
AND    IFNULL(B.status_r, 0) <> 0
AND  (
    B.indice > (SELECT COALESCE(MAX(contador), 0) FROM flybackDW.tblInicioSolicitados)
    OR B.updateAt > (SELECT COALESCE(MAX(Create_At), '2000-01-01') FROM flybackDW.tblInicioSolicitados)
);
```

---

## 📅 Historial

| Versión | Fecha | Cambio |
|---|---|---|
| 1.0 | 2026-06-14 | Creación inicial |
| 2.0 | 2026-06-16 | Agregadas columnas `clientid`, `pack`, `monto`, `currency` |
| 2.1 | 2026-06-18 | Log migrado a `etl_audit_log`. Horario cambiado a 5:30am para evitar deadlock |
