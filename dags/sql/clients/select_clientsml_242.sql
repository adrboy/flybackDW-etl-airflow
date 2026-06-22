-- ═══════════════════════════════════════════════════════
-- SELECT: select_clientsml_242.sql
-- Origen: MariaDB 242 — db_general.viewclientsml
-- Destino: SQL Server — source.clientsml
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
FROM   db_general.viewclientsml
WHERE  clientid > {max_id}
