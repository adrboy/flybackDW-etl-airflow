-- ═══════════════════════════════════════════════════════
-- SELECT: select_clientsfi_240.sql
-- Origen: MariaDB 240 — db_general.viewclientsfi
-- Destino: SQL Server — source.clientsfi
-- Parámetro: {max_id} — se reemplaza en tiempo de ejecución
-- v3 — 2026-08-19: 20 columnas — createdAt/updatedAt/deletedAt
--                  las agrega etl_base.py automáticamente
-- v4 — 2026-08-19: capdata casteado a DATE para compatibilidad SQL Server
-- ═══════════════════════════════════════════════════════
SELECT productid
     , contractid
     , clientid
     , email
     , CAST(capdata AS DATE) AS capdata
     , FirstName
     , LastName
     , countrycode
     , country
     , Estate
     , Ciudad
     , address
     , zip
     , Corpcode
     , Corp
     , ingreso
     , egreso
     , rank
     , EstatusN
     , EstatusL
FROM   db_general.viewclientsfi
WHERE  clientid > {max_id}
