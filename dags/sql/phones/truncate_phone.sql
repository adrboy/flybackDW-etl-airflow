-- ═══════════════════════════════════════════════════════
-- TRUNCATE: truncate_phone.sql
-- Destino: SQL Server — source.Phone (bb/fb/ml/fi/vc)
-- Parámetro: {tabla_destino} — se reemplaza en tiempo de ejecución
-- Nota: commit inmediato — no forma parte de la transacción principal
-- ═══════════════════════════════════════════════════════
TRUNCATE TABLE {tabla_destino}
