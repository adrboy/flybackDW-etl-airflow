-- ═══════════════════════════════════════════════════════
-- INSERT: insert_clientsfb_242.sql
-- Destino: SQL Server — source.clientsfb
-- Nota: placeholders para executemany (pymssql)
-- ═══════════════════════════════════════════════════════
INSERT INTO source.clientsfb
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
