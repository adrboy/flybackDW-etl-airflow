-- ═══════════════════════════════════════════════════════
-- INSERT: insert_clientsml_242.sql
-- Destino: SQL Server — source.clientsml
-- Nota: placeholders para executemany (pymssql)
-- ═══════════════════════════════════════════════════════
INSERT INTO source.clientsml
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
