# log_estadisticas.py — Registro histórico de cada run

## ¿Qué hace?

Registra en `db_general.complete_details` los conteos de clientes de cada
BD origen al momento de la sincronización. Es el historial de cada run —
permite comparar el crecimiento de clientes mes a mes.
Se ejecuta en **Fase 2**, como penúltima operación antes de `notificar`.

---

## Datos técnicos

| Campo | Valor |
|---|---|
| Archivo | `etl_datasync/operations/log_estadisticas.py` |
| Función | `registrar_estadisticas(dag_id)` |
| Servidores | 240 (GC, VTW) y 242 (FB, BB, db_general) |
| Conexiones | `MariaDB240` y `MariaDB` |
| Tiempo promedio | ~1 segundo |

---

## Archivos SQL

| Archivo | Propósito |
|---|---|
| `sql/datasync/insert_update_tables.sql` | Genera `update_id` del run |
| `sql/datasync/select_count_gc.sql` | Cuenta contratos en `financiamiento.credits` |
| `sql/datasync/select_count_fb.sql` | Cuenta clientes en `customers.fb_clients` |
| `sql/datasync/select_count_bb.sql` | Cuenta clientes en `buyback.clients` |
| `sql/datasync/select_count_vtw.sql` | Cuenta registros en `vtw.p_data` |
| `sql/datasync/update_complete_details.sql` | Actualiza conteos en `complete_details` |

---

## Flujo interno

```
1. INSERT INTO update_tables(action=1)
        ↓ trigger automático en MariaDB
        → INSERT complete_details(update_id)       ← fila nueva
        → gcInserted = COUNT(gusa_collections)     ← filas insertadas
        → fbInserted = COUNT(flyback)
        → bbInserted = COUNT(buyback)
        → vtwInserted = COUNT(vtw)

2. COUNT en cada BD origen
        → gc  = COUNT(financiamiento.credits)      ← clientes totales origen
        → fb  = COUNT(customers.fb_clients)
        → bb  = COUNT(buyback.clients)
        → vtw = COUNT(vtw.p_data)

3. UPDATE complete_details SET gc=X, fb=X, bb=X, vtw=X
```

---

## Verificación post-ejecución

```sql
SELECT * FROM db_general.complete_details ORDER BY id DESC LIMIT 3;
```

---

## Ejecución manual

```powershell
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_log_estadisticas.py
```

---

## Notas importantes

- Solo debe ejecutarse si toda la Fase 1 y Fase 2 anterior salieron `success`.
- El trigger `dbgeneral` en `update_tables` crea automáticamente la fila en
  `complete_details` y llena los campos `Inserted` con los conteos de las
  tablas intermedias.
- Fiel al método `GenerateLogsRegistros()` del C# original.
- **Pendiente:** reemplazar por el sistema de log robusto del proyecto
  (`audit_logger`) en la próxima iteración mensual.
