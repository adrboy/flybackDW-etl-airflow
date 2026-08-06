-- 1. Verificar conteos de las 4 tablas fuente
SELECT 'gusa_collections' tabla, COUNT(*) registros FROM db_general.gusa_collections
UNION ALL
SELECT 'flyback',          COUNT(*) FROM db_general.flyback
UNION ALL
SELECT 'buyback',          COUNT(*) FROM db_general.buyback
UNION ALL
SELECT 'vtw',              COUNT(*) FROM db_general.vtw
UNION ALL
SELECT 'complete',         COUNT(*) FROM db_general.complete;

-- 2. Verificar que complete_details tiene el registro más reciente
SELECT * FROM db_general.complete_details 
ORDER BY id DESC LIMIT 3;