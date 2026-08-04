# validate_country.py — Normalización de country_code

## ¿Qué hace?

Normaliza el campo `country_code` en `db_general.complete` después de que
`generate_data` consolida las 4 tablas. El `GROUP_CONCAT` del UNION puede
producir strings como `"USAMEX"` o `"USAUSA"` — esta operación los limpia.
Se ejecuta en **Fase 2**, después de `generate_data`.

---

## Datos técnicos

| Campo | Valor |
|---|---|
| Archivo | `etl_datasync/operations/validate_country.py` |
| Función | `validar_country(dag_id)` |
| Servidor | `192.168.10.242` — `db_general.complete` |
| Conexión | `MariaDB` (`ORIGEN_CONN_ID_242`) |
| Tiempo promedio | ~4 segundos |

---

## Archivos SQL

| Archivo | Propósito |
|---|---|
| `sql/datasync/update_validate_country_usa.sql` | Paso 1 — fuerza `USA` si el string lo contiene |
| `sql/datasync/update_validate_country_trim.sql` | Paso 2 — recorta a 3 chars los demás |

---

## Lógica de normalización

```
Paso 1: country_code REGEXP 'USA' → country_code = 'USA'
        Ejemplo: 'USAMEX' → 'USA'
        Ejemplo: 'USAUSA' → 'USA'

Paso 2: LENGTH(country_code) > 3 AND NOT REGEXP 'USA' → LEFT(3)
        Ejemplo: 'MEXARG' → 'MEX'
        Ejemplo: 'GBRGBR' → 'GBR'
```

**Regla de negocio:** si el cliente tiene productos en USA, se considera cliente USA.

---

## Ejecución manual

```powershell
docker exec -it airflow_scheduler python /opt/airflow/dags/etl_datasync/scripts/run_validate_country.py
```

---

## Notas importantes

- Operación de UPDATE — no trunca ni inserta, solo modifica registros existentes.
- Usa `MySqlHook` — operación interna en un solo servidor.
- Si los dashboards no filtran por `country_code` esta operación puede omitirse.
