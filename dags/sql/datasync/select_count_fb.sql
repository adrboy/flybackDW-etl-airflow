-- ═══════════════════════════════════════════════════════
-- select_count_fb.sql
-- Objetivo : Contar clientes en customers.fb_clients (origen FB)
-- Origen   : 192.168.10.240  (customers)
-- Versión  : 1.0 — 2026-07-30
-- ═══════════════════════════════════════════════════════
SELECT COUNT(1) AS total
FROM customers.fb_clients
