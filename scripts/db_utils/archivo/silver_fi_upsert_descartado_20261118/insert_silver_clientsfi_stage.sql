-- ═══════════════════════════════════════════════════════
-- insert_silver_clientsfi_stage.sql   (NUEVO, borrador)
-- Destino : SQL Server — #clientsfi_stage (tabla temporal de la sesión)
-- Uso     : etl_silver_da.ejecutar_silver (solo fuentes con upsert_stage)
-- Nota    : es el mismo INSERT de siempre (23 columnas, 23 valores), pero
--           contra la tabla de paso. Los tipos de los parámetros salen de
--           #clientsfi_stage, que copia los de source.clientsfi.
-- ═══════════════════════════════════════════════════════
INSERT INTO #clientsfi_stage
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
VALUES ( ?, ?, ?
       , ?, ?
       , ?, ?
       , ?, ?
       , ?, ?
       , ?, ?
       , ?, ?
       , ?, ?, ?
       , ?, ?
       , ?, ?, ?)
