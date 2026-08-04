# vacation_center.py — Sincronización de clientes Vacation Center

## ¿Qué hace?

Lee todos los registros de `vtw.p_data` (servidor 240) y los sincroniza
en `db_general.vtw` (servidor 242). Se ejecuta en **Fase 1** del DAG
junto con las otras 3 operaciones en paralelo.

---

## Datos técnicos

| Campo | Valor |
|---|---|
| Archivo | `etl_datasync/operations/vacation_center.py` |
| Función | `sincronizar_vacation_center(dag_id)` |
| Origen | `192.168.10.240` — `vtw.p_data` |
| Destino | `192.168.10.242` — `db_general.vtw` |
| Conexión origen | `MariaDB_vtw` |
| Conexión destino | `MariaDB_global` |
| Registros aprox. | ~58,000 |
| Tiempo promedio | ~1 min 27 seg |

---

## Archivos SQL

| Archivo | Propósito |
|---|---|
| `sql/datasync/truncate_vacation_center.sql` | Limpia la tabla antes de insertar |
| `sql/datasync/select_vacation_center.sql` | Lee registros desde `vtw.p_data` |
| `sql/datasync/insert_vacation_center.sql` | Inserta en `db_general.vtw` |

---

## Query destacado

El SELECT usa 1 CTE para pre-agregar pagos:

- `fee` — pre-agrega `vtw.payments` por `tradedid` con `statusn IN (2,3,8)`

`tradedid` es el equivalente a `clientid` en este sistema — `p_data` tiene
1 registro por `tradedid` confirmado. Sin `GROUP BY` al final.

---

## Ejecución manual

```powershell
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_vacation_center.py
```

Con medición de tiempo:
```powershell
Measure-Command { docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_vacation_center.py }
```

---

## Log actual

```sql
INSERT INTO db_general.log (description) VALUES ('insert vtw')
```

> **Pendiente:** reemplazar por el sistema de log robusto del proyecto
> (`audit_logger`) en la próxima iteración mensual.

---

## Notas importantes

- `capdata` fluye como `NULL` natural — 0 nulos y 0 centinelas confirmados.
- La conexión de origen (240) se cierra explícitamente antes del INSERT en destino (242).
- `worldid` = contrato, `tradedid` = clientid en la nomenclatura de este sistema.
