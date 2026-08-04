# gusa_collections.py — Sincronización de clientes Gusa Capital

## ¿Qué hace?

Lee todos los contratos de `financiamiento.credits` (servidor 240) y los sincroniza
en `db_general.gusa_collections` (servidor 242). Se ejecuta en **Fase 1** del DAG
junto con las otras 3 operaciones en paralelo.

---

## Datos técnicos

| Campo | Valor |
|---|---|
| Archivo | `etl_datasync/operations/gusa_collections.py` |
| Función | `sincronizar_gusa_collections(dag_id)` |
| Origen | `192.168.10.240` — `financiamiento.credits` |
| Destino | `192.168.10.242` — `db_general.gusa_collections` |
| Conexión origen | `MariaDB_gusa` |
| Conexión destino | `MariaDB_global` |
| Registros aprox. | ~60,000 |
| Tiempo promedio | ~1 min 34 seg |

---

## Archivos SQL

| Archivo | Propósito |
|---|---|
| `sql/datasync/truncate_gusa_collections.sql` | Limpia la tabla antes de insertar |
| `sql/datasync/select_gusa_collections.sql` | Lee contratos desde `financiamiento` |
| `sql/datasync/insert_gusa_collections.sql` | Inserta en `db_general.gusa_collections` |

---

## Query destacado

El SELECT usa 4 CTEs para evitar subconsultas correlacionadas y mejorar rendimiento:

- `cobranza` — pre-agrega `credits_collec` (1.3M filas) por `client_id`
- `saldo` — pre-agrega `credits_collec_ta` para saldo final
- `email` — pre-agrega `clients_email` por `client_id`
- `telefono` — pre-agrega `clients_phone` por `client_id`

Sin los CTEs el query causaba **timeout de 37 minutos**. Con CTEs tarda **~3 segundos**.

---

## Ejecución manual

```powershell
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_gusa_collections.py
```

Con medición de tiempo:
```powershell
Measure-Command { docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_gusa_collections.py }
```

---

## Log actual

```sql
INSERT INTO db_general.log (description) VALUES ('insert gusa_collections')
```

> **Pendiente:** reemplazar por el sistema de log robusto del proyecto
> (`audit_logger`) en la próxima iteración mensual.

---

## Notas importantes

- `sign` fluye como `NULL` natural desde la BD — no requiere transformación.
- La conexión de origen (240) se cierra explícitamente antes del INSERT en destino (242).
- El cuello de botella histórico era `credits_collec` con 1.3M filas — resuelto con CTE.
