-- ═══════════════════════════════════════════════════════
-- INSERT: insert_clientsfi_240.sql
-- Destino: SQL Server — source.clientsfi
-- Nota: placeholders para executemany (pymssql)
-- ═══════════════════════════════════════════════════════
INSERT INTO source.clientsfi
       ( productid, contractid, clientid, email, capdata
       , FirstName, LastName, countrycode, country, Estate
       , Ciudad, address, zip, Corpcode, Corp
       , ingreso, egreso, rank, EstatusN, EstatusL
       , createdAt, updatedAt, deletedAt)
VALUES ( %s, %s, %s, %s, %s
       , %s, %s, %s, %s, %s
       , %s, %s, %s, %s, %s
       , %s, %s, %s, %s, %s
       , %s, %s, %s)
