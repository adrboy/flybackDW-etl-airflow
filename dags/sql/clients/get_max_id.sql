-- ═══════════════════════════════════════════════════════
-- GET MAX ID: get_max_id.sql
-- Objetivo: Obtener el MAX(clientid) del destino
-- Parámetro: {tabla_destino} — se reemplaza en tiempo de ejecución
-- ═══════════════════════════════════════════════════════
SELECT ISNULL(MAX(clientid), 0)
FROM   {tabla_destino}
