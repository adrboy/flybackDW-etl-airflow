-- ═══════════════════════════════════════════════════════
-- update_validate_country_trim.sql
-- Objetivo : Paso 2 — recortar a 3 chars los que no son USA
-- Tabla    : db_general.complete  (servidor 242)
-- Versión  : 1.0 — 2026-07-30
-- ═══════════════════════════════════════════════════════
UPDATE db_general.complete
SET    country_code = LEFT(country_code, 3)
WHERE  id IN (
    SELECT id
    FROM   db_general.complete
    WHERE  LENGTH(country_code) > 3
    AND    country_code NOT REGEXP 'USA'
)
