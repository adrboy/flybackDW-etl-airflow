# flyback.py — Sincronización de clientes Flyback

## ¿Qué hace?

Lee todos los clientes de `customers.fb_clients` (servidor 242) y los sincroniza
en `db_general.flyback` (servidor 242). Se ejecuta en **Fase 1** del DAG junto
con las otras 3 operaciones en paralelo.

---

## Datos técnicos

| Campo | Valor |
|---|---|
| Archivo | `etl_datasync/operations/flyback.py` |
| Función | `sincronizar_flyback(dag_id)` |
| Origen | `192.168.10.242` — `customers.fb_clients` |
| Destino | `192.168.10.242` — `db_general.flyback` |
| Conexión origen | `MariaDB_flyback` |
| Conexión destino | `MariaDB_global` |
| Registros aprox. | ~378,000 |
| Tiempo promedio | ~7 minutos |

---

## Archivos SQL

| Archivo | Propósito |
|---|---|
| `sql/datasync/truncate_flyback.sql` | Limpia la tabla antes de insertar |
| `sql/datasync/select_flyback.sql` | Lee clientes desde `customers.fb_clients` |
| `sql/datasync/insert_flyback.sql` | Inserta en `db_general.flyback` |

---

## Query destacado

El SELECT usa 3 CTEs para evitar subconsultas correlacionadas:

- `corp` — pre-agrega `develops` + `devcorps` para el corporativo
- `tipo_cambio` — pre-agrega `cat_date` para el tipo de cambio por año
- `redeem` — pre-agrega `redeems` para el número máximo de redeem

Sin los CTEs el query tardaba **26 segundos**. Con CTEs tarda **~20 segundos**.

---

## Ejecución manual

```powershell
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_flyback.py
```

Con medición de tiempo:
```powershell
Measure-Command { docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_flyback.py }
```

---

## Log actual

```sql
INSERT INTO db_general.log (description) VALUES ('insert flyback')
```

> **Pendiente:** reemplazar por el sistema de log robusto del proyecto
> (`audit_logger`) en la próxima iteración mensual.

---

## Notas importantes

- `sign` y `activated` llegan como `NULL` natural desde la BD — no requieren
  transformación en Python.
- La conexión de origen se **cierra explícitamente** después del `fetchall()`
  y antes del INSERT — requerido por `mysql-connector-python` con `use_pure=True`.
- Es la operación más lenta de Fase 1 por el volumen (~378K registros).
  El cuello de botella es la escritura en red, limitada por
  `max_allowed_packet = 16MB` del servidor.
