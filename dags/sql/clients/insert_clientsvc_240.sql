-- ═══════════════════════════════════════════════════════
-- INSERT: insert_clientsvc_240.sql
-- Destino: SQL Server — source.clientsvc
-- Nota: placeholders para executemany (pymssql)
-- ═══════════════════════════════════════════════════════
INSERT INTO source.clientsvc
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
