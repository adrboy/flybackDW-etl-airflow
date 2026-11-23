-- ═══════════════════════════════════════════════════════
-- insert_silver_clientsfi_faltantes.sql   (NUEVO, borrador)
-- Destino : source.clientsfi (SQL Server)
-- Uso     : etl_silver_da.ejecutar_silver (solo fuentes con upsert_stage)
-- Proceso : inserta en Silver SOLO los clientes que no existen todavía
--           (NOT EXISTS por clientid). Idempotente: repetirlo da 0 filas.
--           Es un único INSERT ... SELECT, así que su rowcount es el número
--           REAL de filas insertadas (el de executemany contaba filas leídas).
-- ═══════════════════════════════════════════════════════
INSERT INTO source.clientsfi
       ( productid, contractid, clientid
       , email, capdata
       , FirstName, LastName
       , countrycode, country
       , Estate, ciudad
       , address, zip
       , corpcode, corp
       , ingreso, egreso, rank
       , EstatusN, EstatusL
       , createdAt, updatedAt, deletedAt)
SELECT   s.productid, s.contractid, s.clientid
       , s.email, s.capdata
       , s.FirstName, s.LastName
       , s.countrycode, s.country
       , s.Estate, s.ciudad
       , s.address, s.zip
       , s.corpcode, s.corp
       , s.ingreso, s.egreso, s.rank
       , s.EstatusN, s.EstatusL
       , s.createdAt, s.updatedAt, s.deletedAt
FROM   #clientsfi_stage AS s
WHERE  NOT EXISTS (
           SELECT 1
           FROM   source.clientsfi AS t
           WHERE  t.clientid = s.clientid
       )
