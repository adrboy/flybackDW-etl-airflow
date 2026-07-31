-- ═══════════════════════════════════════════════════════
-- select_count_vtw.sql
-- Objetivo : Contar registros en vtw.p_data (origen VTW)
-- Origen   : 192.168.10.240  (vtw)
-- Versión  : 1.0 — 2026-07-30
-- ═══════════════════════════════════════════════════════
SELECT COUNT(1) AS total
FROM vtw.p_data
