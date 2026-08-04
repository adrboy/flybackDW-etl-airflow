# buyback.py — Sincronización de clientes Buyback

## ¿Qué hace?

Lee todos los clientes de `buyback.clients` (servidor 242) y los sincroniza
en `db_general.buyback` (servidor 242). Se ejecuta en **Fase 1** del DAG
junto con las otras 3 operaciones en paralelo.

---

## Datos técnicos

| Campo | Valor |
|---|---|
| Archivo | `etl_datasync/operations/buyback.py` |
| Función | `sincronizar_buyback(dag_id)` |
| Origen | `192.168.10.242` — `buyback.clients` |
| Destino | `192.168.10.242` — `db_general.buyback` |
| Conexión origen | `MariaDB_buyback` |
| Conexión destino | `MariaDB_global` |
| Registros aprox. | ~7,900 |
| Tiempo promedio | ~18 segundos |

---

## Archivos SQL

| Archivo | Propósito |
|---|---|
| `sql/datasync/truncate_buyback.sql` | Limpia la tabla antes de insertar |
| `sql/datasync/select_buyback.sql` | Lee clientes desde `buyback.clients` |
| `sql/datasync/insert_buyback.sql` | Inserta en `db_general.buyback` |

---

## Query destacado

El SELECT es directo sin CTEs — `buyback.clients` tiene 1 registro por `clientid`
confirmado con `COUNT(*) = COUNT(DISTINCT clientid)`. Sin `GROUP BY` ni `SUM`
innecesarios.

`activated` usa `NULLIF(c.factivacion, '0001-01-01')` — 98 registros tienen fecha
centinela del C# original que se convierten a `NULL`.

---

## Ejecución manual

```powershell
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_buyback.py
```

Con medición de tiempo:
```powershell
Measure-Command { docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_buyback.py }
```

---

## Log actual

```sql
INSERT INTO db_general.log (description) VALUES ('insert buyback')
```

> **Pendiente:** reemplazar por el sistema de log robusto del proyecto
> (`audit_logger`) en la próxima iteración mensual.

---

## Notas importantes

- La operación más rápida de Fase 1 por su bajo volumen (~7,900 registros).
- `sign` fluye como `NULL` natural. `activated` usa `NULLIF` en el SQL.
- La conexión de origen se cierra explícitamente antes del INSERT en destino.
