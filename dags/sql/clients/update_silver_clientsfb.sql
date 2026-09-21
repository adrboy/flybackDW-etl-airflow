-- ═══════════════════════════════════════════════════════
-- update_silver_clientsfb.sql
-- Destino : source.clientsfb (SQL Server — pyodbc)
-- Proceso : Silver UPDATE registros modificados
-- Versión : 1.0 — 2026-09-19
-- ═══════════════════════════════════════════════════════
UPDATE source.clientsfb
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
