-- ═══════════════════════════════════════════════════════
-- select_count_bb.sql
-- Objetivo : Contar clientes en buyback.clients (origen BB)
-- Origen   : 192.168.10.242  (buyback)
-- Versión  : 1.0 — 2026-07-30
-- ═══════════════════════════════════════════════════════
SELECT COUNT(1) AS total
FROM buyback.clients
