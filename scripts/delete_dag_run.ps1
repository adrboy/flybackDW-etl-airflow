# ============================================================
# delete_dag_run.ps1
# Limpia dag_run eliminando solo los registros que ya
# fueron archivados en dag_run_historico.
# Uso: ejecutar DESPUES de archive_dag_run.ps1
# Fecha: 2026-06-26
# ============================================================

# ── Configuración — cambia aquí si migras a servidor sin Docker
$PSQL_CMD = "docker exec -it airflow_postgres_dedicated psql -U airflow -c"
# $PSQL_CMD = "psql -h localhost -p 5432 -U airflow -d airflow -c"  # ← sin Docker

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  delete_dag_run.ps1 — Inicio" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# ── Paso 1: Ver qué hay archivado ───────────────────────
Write-Host "`n[1] Verificando dag_run_historico..." -ForegroundColor Yellow
docker exec -it airflow_postgres_dedicated psql -U airflow -c @"
SELECT archived_date, state, COUNT(*) 
FROM dag_run_historico 
GROUP BY archived_date, state
ORDER BY archived_date DESC;
"@

# ── Paso 2: Ver cuántos runs se van a borrar ────────────
Write-Host "`n[2] Runs a eliminar (ya archivados)..." -ForegroundColor Yellow
docker exec -it airflow_postgres_dedicated psql -U airflow -c @"
SELECT COUNT(*) AS runs_a_borrar
FROM dag_run D
WHERE EXISTS (
    SELECT 1 FROM dag_run_historico H
    WHERE H.id = D.id
);
"@

# ── Paso 3: Borrar solo los ya archivados ───────────────
Write-Host "`n[3] Eliminando runs archivados de dag_run..." -ForegroundColor Yellow
docker exec -it airflow_postgres_dedicated psql -U airflow -c @"
DELETE FROM dag_run D
WHERE EXISTS (
    SELECT 1 FROM dag_run_historico H
    WHERE H.id = D.id
);
"@

# ── Paso 4: Verificar lo que queda en dag_run ───────────
Write-Host "`n[4] Runs restantes en dag_run (activos/running)..." -ForegroundColor Yellow
docker exec -it airflow_postgres_dedicated psql -U airflow -c @"
SELECT dag_id, state, COUNT(*) 
FROM dag_run 
GROUP BY dag_id, state
ORDER BY dag_id;
"@

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "  Limpieza completada OK" -ForegroundColor Green
Write-Host "  Los runs 'running' o 'queued' no fueron tocados" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
