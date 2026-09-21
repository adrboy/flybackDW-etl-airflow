-- ═══════════════════════════════════════════════════════
-- update_silver_clientsml.sql
-- Destino : source.clientsml (SQL Server — pyodbc)
-- Proceso : Silver UPDATE registros modificados
-- Versión : 1.0 — 2026-09-21
-- ═══════════════════════════════════════════════════════
UPDATE source.clientsml
SET    productid   = ?
     , contractid  = ?
     , email       = ?
     , capdata     = ?
     , FirstName   = ?
     , LastName    = ?
     , countrycode = ?
     , country     = ?
     , Estate      = ?
     , ciudad      = ?
     , address     = ?
     , zip         = ?
     , corpcode    = ?
     , corp        = ?
     , ingreso     = ?
     , egreso      = ?
     , rank        = ?
     , EstatusN    = ?
     , EstatusL    = ?
     , updatedAt   = ?
     , deletedAt   = ?
WHERE  clientid    = ?
