-- ═══════════════════════════════════════════════════════
-- INSERT: insert_clientsbb_242.sql
-- Destino: SQL Server — source.clientsbb
-- Nota: los %s son placeholders para executemany
-- ═══════════════════════════════════════════════════════
INSERT INTO source.clientsbb
       ( productid, contractid, clientid, email, capdata
       , FirstName, LastName, countrycode, country, Estate
       , ciudad, address, zip, corpcode, corp
       , ingreso, egreso, rank, EstatusN, EstatusL
       , createdAt, updatedAt, deletedAt)
VALUES ( %s, %s, %s, %s, %s
       , %s, %s, %s, %s, %s
       , %s, %s, %s, %s, %s
       , %s, %s, %s, %s, %s
       , %s, NULL, NULL)
