# Convenciones del Proyecto — DockersETL
**Fecha:** 2026-06-26  
**Stack:** Airflow 2.9.3 + MariaDB 242 + SQL Server 244

---

## 1. DAGs — Reglas de oro

### catchup y start_date
```python
# SIEMPRE así por defecto al crear un DAG nuevo
catchup    = False
start_date = datetime(YYYY, MM, DD)  # ← fecha de HOY cuando se crea el DAG
```

**¿Por qué?** Si se borra el historial de `dag_run` y `catchup=True`, Airflow
dispara todos los runs pendientes desde `start_date` automáticamente — genera
runs no deseados y difíciles de limpiar.

**Si algún día necesitas catchup:**
1. Cambiar `start_date` a la fecha desde donde quieres reconstruir
2. Activar `catchup = True`
3. Cuando termina de ponerse al día → volver a `catchup = False` y `start_date = hoy`

---

### Estructura mínima de un DAG nuevo
```python
with DAG(
    dag_id            = "nombre_descriptivo",
    description       = "Qué hace este DAG",
    schedule_interval = "0 6 * * 1",   # ← cron expression
    start_date        = datetime(2026, 6, 26),  # ← fecha de creación
    catchup           = False,          # ← SIEMPRE False por defecto
    tags              = ["tag1", "tag2"],
) as dag:
```

---

### Schedules estándar del proyecto
| Schedule | Cron | DAGs |
|---|---|---|
| Lunes-Viernes 5:30am | `30 5 * * 1-5` | `dag_tbInicioSolAutPag_diario` |
| Lunes-Viernes 8:00am | `0 8 * * 1-5` | `dag_inicio_r_diario` |
| Primer lunes del mes 6am | `0 6 * * 1#1` | `dag_masterclients`, `dag_masterphones` |
| Primer lunes del mes 7am | `0 7 * * 1#1` | `dag_master_gold` |
| Cada lunes 6am | `0 6 * * 1` | `flybackDW_sp_ActivosRedeemCorp` |
| Día 1 de cada mes 6am | `0 6 1 * *` | `flybackDW_spInsertHistoricoCobranza` |
| Día 2 de cada mes 1am | `0 1 2 * *` | `dag_limpieza_Mensual_tbInicioSolAutPag` |
| 1 febrero 6am | `0 6 1 2 *` | `flybackDW_sp_FinalizarContratosVencidos` |

---

## 2. Scripts — Patrón de desarrollo

### Flujo estándar
```
scripts/db_utils/   ← construir y validar aquí primero
        ↓ validado
dags/               ← promover a DAG de producción
```

**Nunca** desarrollar directamente en `dags/` — siempre pasar por `scripts/` primero.

---

## 3. SQL — Estilo

- Formato comma-first en SELECT
- SQL externo en `dags/sql/` — nunca embebido en el DAG
- Comentarios obligatorios en SPs: versión, fecha, cambios

---

## 4. Limpieza de historial dag_run

**Orden correcto para limpiar sin catchup:**
1. Pausar todos los DAGs
2. Verificar que todos tienen `catchup=False` y `start_date` reciente
3. Borrar `dag_run`
4. Reiniciar scheduler
5. Reactivar DAGs

```sql
-- Pausar todo
UPDATE dag SET is_paused = true WHERE is_paused = false;

-- Limpiar historial
DELETE FROM dag_run;

-- Reactivar producción (excluir tests)
UPDATE dag SET is_paused = false 
WHERE dag_id NOT IN ('0_certificacion_entorno', 'dag_test', 'test_conexion_mariadb', 'test_conexion_mssql244');
```

---

## 5. Drivers ODBC

- **Docker (Airflow):** ODBC Driver 18 for SQL Server — único disponible
- **Windows (Anaconda):** ODBC Driver 18 for SQL Server — instalado 2026-06-24
- **Regla:** siempre usar Driver 18 en ambos entornos — paridad garantizada
