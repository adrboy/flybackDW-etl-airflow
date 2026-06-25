# dag_tbInicioSolAutPag_diario

**Fecha:** 2026-06-25  
**Carpeta:** `dags/etl_flyback/`  
**Schedule:** `30 5 * * 1-5` — Lunes a Viernes 5:30am Cancún  
**Tags:** flybackDW, redeems, mariadb

---

## Objetivo

Actualiza las tres tablas de inicio en `flybackDW` ejecutando sus SPs correspondientes:

| Tabla destino | SP |
|---|---|
| `flybackDW.tblInicioSolicitados` | `update_flybackDW_tblInicioSolicitados_VI_hour` |
| `flybackDW.tblInicioAutorizados` | `update_flybackDW_tblInicioAutorizados_VI_hour` |
| `flybackDW.tblInicioPagados`     | `update_flybackDW_tblInicioPagados_VI_hour`     |

---

## Arquitectura v3.0

### Función reutilizable `ejecutar_sp(tarea: dict)`
Una sola función agnóstica reemplaza las tres funciones idénticas de v2.3.  
Recibe el dict de configuración completo — si mañana se agrega un SP nuevo, solo se agrega un dict a `TAREAS`.

### Catálogo `TAREAS`
Única fuente de verdad del DAG:

```python
TAREAS = [
    {
        "sp"           : "flybackDW.update_flybackDW_tblInicioSolicitados_VI_hour",
        "vista_origen" : "customers.redeems",
        "tabla_destino": "flybackDW.tblInicioSolicitados",
        "sleep_seg"    : 0,
    },
    ...
]
```

### SQL externo
El `INSERT` de auditoría de errores ya no está embebido — vive en:  
`dags/sql/etl_flyback/insert_audit_log_error.sql`

Cargado con `cargar_sql()` de `common/sql_loader.py`.

---

## Secuencia de tareas

```
actualizar_sol → actualizar_aut → actualizar_pag → notificar
```

`tblInicioPagados` tiene `sleep_seg=10` para evitar deadlocks con `tblInicioAutorizados`.

---

## Historial de versiones

| Versión | Fecha | Cambio |
|---|---|---|
| 2.3 | 2026-06-19 | try/except en las 3 tareas + sleep anti-deadlock |
| 3.0 | 2026-06-25 | `ejecutar_sp()` reutilizable + SQL externo, sin SQL embebido |
