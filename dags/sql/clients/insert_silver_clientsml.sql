-- ═══════════════════════════════════════════════════════
-- insert_silver_clientsml.sql
-- Destino : source.clientsml (SQL Server — pyodbc)
-- Proceso : Silver INSERT nuevos registros
-- Versión : 1.0 — 2026-09-21
-- ═══════════════════════════════════════════════════════
INSERT INTO source.clientsml
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
