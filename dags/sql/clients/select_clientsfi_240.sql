-- ═══════════════════════════════════════════════════════
-- SELECT: select_clientsfi_240.sql
-- Origen: MariaDB 240 — db_general.viewclientsfi
-- Destino: SQL Server — source.clientsfi
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
FROM   db_general.viewclientsfi
WHERE  clientid > {max_id}
