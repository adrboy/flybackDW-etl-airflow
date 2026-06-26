# ============================================================
# archive_dag_run.ps1
# Archiva los runs de dag_run en dag_run_historico
# antes de hacer limpieza del historial.
# Uso: ejecutar ANTES de hacer DELETE FROM dag_run
# Fecha: 2026-06-26
# ============================================================

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "  archive_dag_run.ps1 — Inicio" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan

# ── Paso 1: Ver cuántos runs hay antes de archivar ──────
Write-Host "`n[1] Auditando dag_run actual..." -ForegroundColor Yellow
docker exec -it airflow_postgres_dedicated psql -U airflow -c @"
SELECT dag_id, state, COUNT(*) 
FROM dag_run 
GROUP BY dag_id, state 
ORDER BY dag_id, state;
"@

# ── Paso 2: Archivar en dag_run_historico ───────────────
Write-Host "`n[2] Archivando en dag_run_historico..." -ForegroundColor Yellow
docker exec -it airflow_postgres_dedicated psql -U airflow -c @"
INSERT INTO dag_run_historico
       ( id, dag_id, queued_at, execution_date
       , start_date, end_date, state, run_id, run_type
       , archived_at, archived_week, archived_date)
SELECT   id, dag_id, queued_at, execution_date
       , start_date, end_date, state, run_id, run_type
       , NOW()
       , EXTRACT(WEEK FROM NOW())::INTEGER
       , CURRENT_DATE
FROM   dag_run
WHERE  state IN ('failed', 'success');
"@

# ── Paso 3: Verificar cuántos se archivaron ─────────────
Write-Host "`n[3] Verificando historico..." -ForegroundColor Yellow
docker exec -it airflow_postgres_dedicated psql -U airflow -c @"
SELECT archived_date, state, COUNT(*) 
FROM dag_run_historico 
GROUP BY archived_date, state
ORDER BY archived_date DESC;
"@

Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "  Archivado OK - Ahora puedes limpiar dag_run" -ForegroundColor Green
Write-Host "  DELETE FROM dag_run;" -ForegroundColor Green
Write-Host "============================================" -ForegroundColor Cyan
