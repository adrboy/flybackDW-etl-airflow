-- ═══════════════════════════════════════════════════════
-- INSERT: insert_phone.sql
-- Destino: SQL Server — source.Phone (bb/fb/ml/fi/vc)
-- Parámetro: {tabla_destino} — se reemplaza en tiempo de ejecución
-- Nota: placeholders para pyodbc (? en lugar de %s)
-- atUpdate = NULL — TRUNCATE+INSERT, no hay update
-- ═══════════════════════════════════════════════════════
INSERT INTO {tabla_destino}
       ( clientid, phone, atInsert, atUpdate)
VALUES ( ?, ?, ?, NULL)
