# Auditoría — flybackDW_spInsertHistoricoCobranza

> **DAG:** `flybackDW_spInsertHistoricoCobranza`
> **Schedule:** Día 1 de cada mes a las 6:00am
> **Responsable:** Andrés José Sarria Correa
> **Última actualización:** 2026-07-03

---

## Flujo de verificación (seguir en orden)

```
1. etl_audit_log  → estado OK + filas_insertadas
2. dag_run        → state = success
3. Correo         → ETL Notification OK
4. Log .txt       → FIN ✅
5. Auditoría SQL  → diff_cnt=0 y diff_monto=0.00
```

---

## Registro de ejecuciones

### 2026-07-01 — Mes procesado: 202606

#### 1. etl_audit_log
```
id              : 173
paquete         : spInsertHistoricoCobranza
max_id_inicio   : 202606
filas_insertadas: 0  (ya estaba sincronizado)
estado          : OK
fecha_inicio    : 2026-07-01 06:00:20
fecha_fin       : 2026-07-01 06:00:35
mensaje_error   : (vacío)
```

#### 2. dag_run (PostgreSQL Airflow)
```
dag_id    : flybackDW_spInsertHistoricoCobranza
state     : success
queued_at : 2026-07-01 06:00:19
start_date: 2026-07-01 06:00:20
end_date  : 2026-07-01 06:00:36
```

#### 3. Correo
```
Proceso : flybackDW_spInsertHistoricoCobranza
Estado  : OK
Fecha   : 2026-07-01 06:00
```

#### 4. Log .txt
```
[2026-07-01 06:00:XX] DAG: flybackDW_spInsertHistoricoCobranza — INICIO
spInsertHistoricoCobranza(NULL) — OK
Tabla: flybackDW.tbl_historico_cobranza
DAG: flybackDW_spInsertHistoricoCobranza — FIN ✅
```

#### 5. Auditoría SQL — BETWEEN 202606 AND 202606
```
pay_year_month  cnt_origen  monto_origen    cnt_destino  monto_destino   diff_cnt  diff_monto
202606          3,569       2,858,793.90    3,569        2,858,793.90    0         0.00  ✅
```

**Resultado:** ✅ Sincronización perfecta

---

<!-- Copiar el bloque anterior para cada nueva ejecución mensual -->
