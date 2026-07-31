-- ═══════════════════════════════════════════════════════
-- select_count_gc.sql
-- Objetivo : Contar contratos en financiamiento (origen GC)
-- Origen   : 192.168.10.240  (financiamiento)
-- Versión  : 1.0 — 2026-07-30
-- ═══════════════════════════════════════════════════════
SELECT COUNT(1) AS total
FROM financiamiento.credits
