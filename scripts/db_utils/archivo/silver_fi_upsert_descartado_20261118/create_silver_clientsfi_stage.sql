-- ═══════════════════════════════════════════════════════
-- create_silver_clientsfi_stage.sql   (NUEVO, borrador)
-- Destino : SQL Server — tabla temporal de la sesión
-- Uso     : etl_silver_da.ejecutar_silver (solo fuentes con upsert_stage)
-- Nota    : crea #clientsfi_stage VACÍA con las mismas columnas y tipos que
--           source.clientsfi (SELECT TOP 0 ... INTO copia tipos, nulabilidad
--           y collation de la tabla real: no hay conflicto de collation).
--           Vive solo en la conexión de la transacción.
-- ═══════════════════════════════════════════════════════
SELECT TOP 0
       productid, contractid, clientid
     , email, capdata
     , FirstName, LastName
     , countrycode, country
     , Estate, ciudad
     , address, zip
     , corpcode, corp
     , ingreso, egreso, rank
     , EstatusN, EstatusL
     , createdAt, updatedAt, deletedAt
INTO   #clientsfi_stage
FROM   source.clientsfi
