-- ═══════════════════════════════════════════════════════
-- update_validate_country_usa.sql
-- Objetivo : Paso 1 — forzar USA cuando el string lo contiene
-- Tabla    : db_general.complete  (servidor 242)
-- Versión  : 1.0 — 2026-07-30
-- ═══════════════════════════════════════════════════════
UPDATE db_general.complete
SET    country_code = 'USA'
WHERE  id IN (
    SELECT id
    FROM   db_general.complete
    WHERE  LENGTH(country_code) > 3
    AND    country_code REGEXP 'USA'
)
