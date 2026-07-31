-- ═══════════════════════════════════════════════════════
-- insert_update_tables.sql
-- Objetivo : Registrar inicio de sincronización
--            Genera el update_id para este run
-- Tabla    : db_general.update_tables  (servidor 242)
-- Versión  : 1.0 — 2026-07-30
-- ═══════════════════════════════════════════════════════
INSERT INTO db_general.update_tables (action)
VALUES (1)
