-- ═══════════════════════════════════════════════════════
-- SELECT: select_clientsml_242.sql
-- Origen: MariaDB 242 — db_general.viewclientsml
-- Destino: SQL Server — source.clientsml
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
FROM   db_general.viewclientsml
WHERE  clientid > {max_id}
