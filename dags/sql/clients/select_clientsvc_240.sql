-- ═══════════════════════════════════════════════════════
-- SELECT: select_clientsvc_240.sql
-- Origen: MariaDB 240 — db_general.viewclientsvc
-- Destino: SQL Server — source.clientsvc
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
