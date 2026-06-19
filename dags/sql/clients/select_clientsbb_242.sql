-- ═══════════════════════════════════════════════════════
-- SELECT: select_clientsbb_242.sql
-- Origen: MariaDB 242 — db_general.viewclientsbb
-- Destino: SQL Server — source.clientsbb
-- Parámetro: {max_id} — se reemplaza en tiempo de ejecución
-- ═══════════════════════════════════════════════════════
SELECT productid
     , contractid
     , clientid
     , email
     , capdata
     , FirstName
     , LastName
     , countrycode
     , country
     , Estate
     , ciudad
     , address
     , zip
     , corpcode
     , corp
     , ingreso
     , egreso
     , rank
     , EstatusN
     , EstatusL
FROM   db_general.viewclientsbb
WHERE  clientid > {max_id}
