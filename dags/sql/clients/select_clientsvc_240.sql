-- ═══════════════════════════════════════════════════════
-- SELECT: select_clientsvc_240.sql
-- Origen: MariaDB 240 — db_general.viewclientsvc
-- Destino: SQL Server — source.clientsvc
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
FROM   db_general.viewclientsvc
WHERE  clientid > {max_id}
