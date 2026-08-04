-- ═══════════════════════════════════════════════════════
-- truncate_buyback.sql
-- Objetivo : Limpiar tabla destino antes de sincronizar
-- Tabla    : db_general.buyback  (servidor 242)
-- Versión  : 1.1 — 2026-08-03
-- Nota     : Una sola sentencia — use_pure=True no acepta multi
-- ═══════════════════════════════════════════════════════
TRUNCATE db_general.buyback
