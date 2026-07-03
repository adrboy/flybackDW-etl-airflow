# Resumen de Trabajo — Proyecto ETL flybackDW
> **Período:** Junio - Julio 2026
> **Equipo:** Andrés José Sarria Correa + CC (Claude)
> **Stack:** Airflow 2.9.3 Docker, MariaDB 242, SQL Server 244, VB.NET ComponentOne
> **Repositorio:** `C:\Users\GUSA CAPITAL\Documents\DockersETL`

---

## 1. Migración ETL — SSIS/KingswaySoft → Airflow

### Logros completados
- Migración masiva completada con patrón v2: SQL externo, `etl_base.py` v2.4, transacciones atómicas con rollback
- Resolución del cuello de botella pymssql (682 RPS) → pyodbc + `fast_executemany=True` con msodbcsql18 logrando **49,891 RPS (mejora 73×)**
- Driver ODBC 18 instalado en Anaconda Windows — paridad total Anaconda ↔ Docker
- DAGs homologados para cinco clientes (bb, fb, fi, ml, vc) y cinco phones
- `dag_master_gold.py` con `ExternalTaskSensor`

### Patrón estándar de DAGs
```
catchup   = False       ← SIEMPRE por defecto
start_date = datetime(YYYY, MM, DD)  ← fecha de HOY al crear
```
> Documentado en `CONVENTIONS.md`

---

## 2. flybackDW — Tablas de inicio Redeems

### Tablas sincronizadas
- `tblInicioSolicitados`, `tblInicioAutorizados`, `tblInicioPagados`
- SPs v3.0 con LEFT JOIN + DATE(updateAt) certificados 2014-2026 (0 diferencias)
- DAG `dag_tbInicioSolAutPag_diario` — L-V 5:30am
- DAG `dag_inicio_r_diario` — migrado desde Batch Navicat, L-V 8:00am

### Motor de Auditoría Genérico
- `scripts/db_utils/audit_engine.py` v2.0 con `sync_config.json`
- Índices `idx_colleclog_reg_fecha` e `idx_colleclog_reg_fecha_fcobrado` — reducen consultas de 32s → 0.4s (**mejora 75×**)

---

## 3. tbl_historico_cobranza — Reconstrucción completa

### Estructura de la tabla
```
(indice, is_packed) — clave compuesta única
is_packed=1 → NO PACKED (cardcollec.indice)
is_packed=2 → PACKED    (carterapagopackeados.id)
```

### Columnas agregadas en V3/V4/V5
| Columna | Descripción |
|---|---|
| `idcc` | cardcollec.indice — para PACKED ≠ indice principal |
| `clientid` | ID del cliente |
| `ModoIN` | 1=ConTarjeta 2=Pack 3=SinTarjeta 4=Tokenizado 5=UVC |
| `agente` | Agente que procesó el cobro |
| `pay_method` | Método de pago (PA/CC/WT/RA/CR/CH/OT) |

### Evolución del SP
| Versión | Fecha | Cambio clave |
|---|---|---|
| V1 | 2026-05-01 | Implementación inicial |
| V2 | 2026-06-02 | Eliminado filtro fcobrado, lógica neta PACKED |
| V3 | 2026-06-29 | Nuevas columnas: idcc, clientid, ModoIN, agente, pay_method |
| V4 | 2026-06-30 | OR colleclog para capturar modificaciones históricas |
| V5 | 2026-07-03 | ROW_NUMBER colleclog + 5 columnas comparadas + pay_date actualizable + etl_audit_log |

### Reconstrucción histórica certificada
```
2013 (ago-dic) → 2026 (ene-jun) : 385,275 registros — 0 diferencias ✅
~$318 millones en histórico de cobranza
```

### OR colleclog (V5) — captura modificaciones históricas
```sql
OR CC.indice IN (
    SELECT cardcollec_id FROM (
        SELECT cardcollec_id, monto, statusc, fcobrado, currency, pay_method,
               ROW_NUMBER() OVER (PARTITION BY cardcollec_id ORDER BY fecha DESC) AS rn
        FROM customers.colleclog
        WHERE reg = 'UPDATE'
        AND EXTRACT(YEAR_MONTH FROM fecha)    = p_year_month
        AND EXTRACT(YEAR_MONTH FROM fcobrado) < p_year_month
        AND fcobrado IS NOT NULL
        AND statusc IN (1,2,3,6,7,9,10,12)
    ) ULT
    LEFT JOIN flybackDW.tbl_historico_cobranza H
           ON H.indice = ULT.cardcollec_id AND H.is_packed = 1
    WHERE ULT.rn = 1
    AND (H.indice IS NULL
         OR COALESCE(ULT.monto,      -1)           <> COALESCE(H.amount_BRUTA_usd, -1)
         OR COALESCE(ULT.statusc,    -1)           <> COALESCE(H.statusc,          -1)
         OR COALESCE(ULT.fcobrado,   '1900-01-01') <> COALESCE(H.pay_date,         '1900-01-01')
         OR COALESCE(ULT.currency,   '')           <> COALESCE(H.currency,          '')
         OR COALESCE(ULT.pay_method, '')           <> COALESCE(H.pay_method,        ''))
)
```

> **Pendiente V6:** cuando se agregue `updateAt` a `customers.cardcollec`, reemplazar todo el subquery por:
> `OR (EXTRACT(YEAR_MONTH FROM CC.updateAt) = p_year_month AND EXTRACT(YEAR_MONTH FROM CC.fcobrado) < p_year_month)`

---

## 4. Estructura de archivos — etl_flyback

```
DockersETL/
├── dags/
│   └── etl_flyback/
│       ├── flybackDW_spInsertHistoricoCobranza.py
│       ├── flybackDW_sp_ActivosRedeemCorp.py
│       ├── flybackDW_sp_FinalizarContratosVencidos.py
│       ├── dag_tbInicioSolAutPag_diario.py
│       ├── dag_inicio_r_diario.py
│       └── dag_limpieza_Mensual_tbInicioSolAutPag.py
├── docs/
│   └── etl_flyback/
│       ├── MANUAL_OPERACIONES.md
│       ├── Manual_Audit_flybackDW_spInsertHistoricoCobranza.md
│       ├── Manual_Audit_dag_tbInicioSolAutPag_diario.md
│       ├── flybackDW_spInsertHistoricoCobranza_v2.md
│       ├── flybackDW_sp_ActivosRedeemCorp.md
│       ├── flybackDW_sp_FinalizarContratosVencidos.md
│       ├── dag_tbInicioSolAutPag_diario.md
│       ├── dag_inicio_r_diario.md
│       └── dag_limpieza_Mensual_tbInicioSolAutPag.md
└── scripts/
    ├── archive_dag_run.ps1
    ├── delete_dag_run.ps1
    ├── cleanup_logs.ps1
    └── auditoria/
        ├── AuditSolicitados.sql
        ├── AuditAutorizados.sql
        └── AuditPagados.sql
```

---

## 5. Tabla de DAGs — Schedules

| DAG | Schedule | Frecuencia |
|---|---|---|
| `dag_tbInicioSolAutPag_diario` | `30 5 * * 1-5` | L-V 5:30am |
| `dag_inicio_r_diario` | `0 8 * * 1-5` | L-V 8:00am |
| `flybackDW_sp_ActivosRedeemCorp` | `0 6 * * 1` | Cada lunes 6am |
| `dag_masterclients` | `0 6 * * 1#1` | Primer lunes mes 6am |
| `dag_masterphones` | `0 6 * * 1#1` | Primer lunes mes 6am |
| `dag_master_gold` | `0 7 * * 1#1` | Primer lunes mes 7am |
| `flybackDW_spInsertHistoricoCobranza` | `0 6 1 * *` | Día 1 mes 6am |
| `dag_limpieza_Mensual_tbInicioSolAutPag` | `0 1 2 * *` | Día 2 mes 1am |
| `flybackDW_sp_FinalizarContratosVencidos` | `0 6 1 2 *` | 1 febrero 6am |

---

## 6. Convenciones clave aprendidas

### MariaDB ALTER TABLE
```sql
-- CORRECTO: AFTER va después del COMMENT
ADD COLUMN col tipo NULL COMMENT 'texto' AFTER otra_col;
```

### NULL en SQL
```sql
-- NULL <> valor = NULL (no TRUE) — usar COALESCE
COALESCE(col, -1) <> COALESCE(VALUES(col), -1)
```

### catchup en Airflow
- `catchup=False` — SIEMPRE por defecto
- `catchup=True` — solo para DAGs mensuales idempotentes (ON DUPLICATE KEY UPDATE)
- Limpiar `dag_run` → pausar DAGs PRIMERO, luego borrar, luego reactivar

### PowerShell
- Separador de comandos: `;` (no `&&`)
- Scripts `.ps1`: `.\scripts\archivo.ps1`
- Salir del paginador psql: `q`

---

## 7. Pendientes activos

| Prioridad | Tarea |
|---|---|
| Alta | Agregar `updateAt` a `customers.cardcollec` con trigger BEFORE UPDATE → habilita SP V6 |
| Alta | Confirmar mapeo `etapa`/`donde` con programadores (card_type, herramienta, country) |
| Media | Migrar `colleclog` fuera de `customers` (informe técnico listo) |
| Media | Auditoría formularios VB.NET onpremises vs tbl_historico_cobranza |
| Media | Redirigir 4 formularios VB.NET cardcollect → flybackDW |
| Baja | Publicar LinkedIn: pymssql vs pyodbc 73x mejora |
| Baja | Actualizar Apache Airflow a versión más reciente |

---

## 8. Comandos frecuentes

```powershell
# Reiniciar scheduler
cd "C:\Users\GUSA CAPITAL\Documents\DockersETL"; docker-compose restart airflow-scheduler

# Ver DAGs activos
docker exec -it airflow_scheduler airflow dags list

# Disparar DAG manual
docker exec -it airflow_scheduler airflow dags trigger <dag_id>

# Ver runs de un DAG
docker exec -it airflow_scheduler airflow dags list-runs -d <dag_id>

# Limpiar logs
cd "C:\Users\GUSA CAPITAL\Documents\DockersETL"; .\scripts\cleanup_logs.ps1

# Archivar dag_run antes de limpiar
cd "C:\Users\GUSA CAPITAL\Documents\DockersETL"; .\scripts\archive_dag_run.ps1
```

---

*Resumen generado 2026-07-03 | Andrés + CC — Gusacapital*
