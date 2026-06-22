-- ═══════════════════════════════════════════════════════
-- SELECT: select_phone.sql
-- Origen: MariaDB — db_general.vwpersonalinfo (bb/fb/ml/fi/vc)
-- Parámetro: {vista_origen} — se reemplaza en tiempo de ejecución
-- ═══════════════════════════════════════════════════════
SELECT clientid
     , PHONE
FROM   {vista_origen}
