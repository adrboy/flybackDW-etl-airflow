-- ═══════════════════════════════════════════════════════
-- update_silver_clientsbb.sql
-- Destino : source.clientsbb (SQL Server — pyodbc)
-- Proceso : Silver UPDATE registros modificados
-- Versión : 1.0 — 2026-09-21
-- ═══════════════════════════════════════════════════════
UPDATE source.clientsbb
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
