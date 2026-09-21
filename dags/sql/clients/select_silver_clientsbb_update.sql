-- ═══════════════════════════════════════════════════════
-- select_silver_clientsbb_update.sql
-- Origen  : db_general.tbl_raw_clientsbb (MariaDB 242)
-- Destino : source.clientsbb (SQL Server)
-- Proceso : Silver UPDATE — registros modificados
-- Modo FULL        : updatedAt > '2000-01-01' (trae todos)
-- Modo INCREMENTAL : updatedAt > {max_updatedat}
-- Nota    : address = NULL — no existe en buyback.clients
-- Versión : 1.0 — 2026-09-21
-- ═══════════════════════════════════════════════════════
SELECT
      raw.productid
    , raw.contractid
    , raw.clientid
    , COALESCE(
          NULLIF(TRIM(raw.ema1), '')
        , NULLIF(TRIM(raw.ema2), '')
        , CONCAT(raw.clientid, '@noreply.flyback')
      )                                                AS email
    , CAST(raw.capdata AS DATE)                        AS capdata
    , raw.fname1                                       AS FirstName
    , raw.lname1                                       AS LastName
    , (SELECT x.iso_iii
       FROM   db_general.tbldimpais x
       WHERE  x.Pais    LIKE CONCAT(raw.country, '%')
          OR  x.pais_in LIKE CONCAT(raw.country, '%')
          OR  x.pais_es LIKE CONCAT(raw.country, '%')
       LIMIT 1)                                        AS countrycode
    , raw.country
    , IF(OCTET_LENGTH(raw.state) > 3
        , LCASE(raw.state), 'other')                   AS Estate
    , COALESCE(IF(OCTET_LENGTH(raw.city) > 3
        AND raw.city <> ''
        , LCASE(raw.city), 'other'), 'other')          AS ciudad
    , NULL                                             AS address
    , raw.zip
    , COALESCE((SELECT CONCAT(x.idcorp, x.Empresa)
                FROM   db_general.dimsalasbb x
                WHERE  x.iddev = raw.company
                ), -1)                                 AS corpcode
    , COALESCE((SELECT xii.corpname
                FROM   db_general.dimcorpbb xii
                WHERE  xii.corplevel IN (
                       SELECT x.idcorp
                       FROM   db_general.dimsalasbb x
                       WHERE  x.iddev = raw.company)
                ), 'INACTIVE')                         AS corp
    , 0                                                AS ingreso
    , 0                                                AS egreso
    , 5                                                AS rank
    , raw.status                                       AS EstatusN
    , COALESCE((SELECT x.descrip
                FROM   db_general.dimestatusml x
                WHERE  x.idstatus = raw.status
                ), 'NaN')                              AS EstatusL
    , CAST(raw.capdata AS DATE)                        AS createdAt
    , raw.updatedAt                                    AS updatedAt
    , raw.deletedAt                                    AS deletedAt
FROM   db_general.tbl_raw_clientsbb  raw
WHERE  raw.deletedAt IS NULL
  AND  raw.clientid  <= {max_id}
  AND  raw.updatedAt >  {max_updatedat}
