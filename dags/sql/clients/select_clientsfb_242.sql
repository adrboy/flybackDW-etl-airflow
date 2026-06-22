-- ═══════════════════════════════════════════════════════
-- SELECT: select_clientsfb_242.sql
-- Origen: MariaDB 242 — db_general.viewclientsfb
-- Destino: SQL Server — source.clientsfb
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
FROM   db_general.viewclientsfb
WHERE  clientid > {max_id}
