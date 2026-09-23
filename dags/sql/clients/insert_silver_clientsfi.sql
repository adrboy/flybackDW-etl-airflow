-- ═══════════════════════════════════════════════════════
-- insert_silver_clientsfi.sql
-- Destino : source.clientsfi (SQL Server — pyodbc)
-- Proceso : Silver INSERT nuevos registros
-- Versión : 1.0 — 2026-09-22
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
