-- ═══════════════════════════════════════════════════════
-- update_silver_clientsfi.sql
-- Destino : source.clientsfi (SQL Server — pyodbc)
-- Proceso : Silver UPDATE registros modificados
-- Versión : 1.0 — 2026-09-22
-- ═══════════════════════════════════════════════════════
UPDATE source.clientsfi
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
