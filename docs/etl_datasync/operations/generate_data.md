# generate_data.py — Consolidación en complete

## ¿Qué hace?

Consolida las 4 tablas intermedias en `db_general.complete` usando un
`INSERT ... SELECT` con UNION. Se ejecuta en **Fase 2** — solo después
de que las 4 operaciones de Fase 1 hayan terminado exitosamente.

---

## Datos técnicos

| Campo | Valor |
|---|---|
| Archivo | `etl_datasync/operations/generate_data.py` |
| Función | `generar_data(dag_id)` |
| Servidor | `192.168.10.242` — todo interno en `db_general` |
| Conexión | `MariaDB` (`ORIGEN_CONN_ID_242`) |
| Registros aprox. | ~391,000 en `complete` |
| Tiempo promedio | ~18 segundos |

---

## Archivos SQL

| Archivo | Propósito |
|---|---|
| `sql/datasync/truncate_complete.sql` | Limpia `complete` antes de consolidar |
| `sql/datasync/insert_complete.sql` | UNION de GC + FB + BB + VTW → `complete` |

---

## Query destacado

`insert_complete.sql` hace un `INSERT ... SELECT` con UNION de 4 fuentes.
Las columnas numéricas usan `SUM` o `MAX` — nunca `GROUP_CONCAT` que
convertiría números a strings incompatibles con los tipos `int` y `decimal`
de la tabla destino.

| Tipo de columna | Función usada |
|---|---|
| `int`, `decimal` | `SUM` |
| `date` | `MAX` |
| `varchar(3)` | `MAX` |
| `text` | `GROUP_CONCAT` |

---

## Ejecución manual

```powershell
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_generate_data.py
```

---

## Notas importantes

- **Dependencia estricta:** no ejecutar si alguna de las 4 tablas fuente está vacía.
- Usa `MySqlHook` en lugar de `mysql-connector-python` — operación interna
  en un solo servidor, sin conflicto de drivers.
- El `cursor.rowcount` no retorna el total correcto después de `INSERT ... SELECT`
  en `MySQLdb` — verificar con `SELECT COUNT(*) FROM db_general.complete`.
