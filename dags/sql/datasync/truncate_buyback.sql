-- ═══════════════════════════════════════════════════════
-- truncate_buyback.sql
-- Objetivo : Limpiar tabla destino antes de sincronizar
-- Tabla    : db_general.buyback  (servidor 242)
-- Versión  : 1.0 — 2026-07-30
-- ═══════════════════════════════════════════════════════
TRUNCATE db_general.buyback;

INSERT INTO db_general.log (description)
VALUES ('truncate buyback');
