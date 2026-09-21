-- ═══════════════════════════════════════════════════════
-- insert_silver_clientsfb.sql
-- Destino : source.clientsfb (SQL Server — pyodbc)
-- Proceso : Silver INSERT nuevos registros
-- Versión : 1.0 — 2026-09-19
-- ═══════════════════════════════════════════════════════
INSERT INTO source.clientsfb
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
