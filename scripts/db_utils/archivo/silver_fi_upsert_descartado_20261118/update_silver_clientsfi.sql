-- ═══════════════════════════════════════════════════════
-- update_silver_clientsfi.sql   (BORRADOR v1.1)
-- Destino : source.clientsfi (SQL Server — pyodbc)
-- Proceso : Silver UPDATE registros modificados
-- Versión : 1.1 — 2026-11-18 (borrador)
--   v1.0: deletedAt = COALESCE(CAST(? AS datetime2(7)), deletedAt)
--         → un deletedAt nunca se podía limpiar en Silver.
--   v1.1: deletedAt = CAST(? AS datetime2(7)). Se copia tal cual desde RAW:
--         si RAW lo pone en NULL (cliente revivido), Silver también.
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
     , deletedAt   = CAST(? AS datetime2(7))
WHERE  clientid    = ?
