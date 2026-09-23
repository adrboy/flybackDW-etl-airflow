-- ═══════════════════════════════════════════════════════
-- select_silver_clientsvc_update.sql
-- Origen  : db_general.tbl_raw_clientsvc (MariaDB 240)
-- Destino : source.clientsvc (SQL Server)
-- Proceso : Silver UPDATE — registros modificados
-- Modo FULL        : updatedAt > '2000-01-01' (trae todos)
-- Modo INCREMENTAL : updatedAt > {max_updatedat}
-- Nota    : tradedid → clientid | pcode → zip | email1/2/3 → COALESCE
-- Versión : 1.0 — 2026-09-22
-- ═══════════════════════════════════════════════════════
SELECT
      raw.productid
    , raw.worldid                                        AS contractid
    , raw.tradedid                                       AS clientid
    , COALESCE(
          NULLIF(TRIM(raw.email1), '')
        , NULLIF(TRIM(raw.email2), '')
        , NULLIF(TRIM(raw.email3), '')
        , CONCAT(raw.tradedid, '@noreply.vacation')
      )                                                  AS email
    , CAST(raw.capdata AS DATE)                          AS capdata
    , raw.fname1                                         AS FirstName
    , raw.lname1                                         AS LastName
    , (SELECT x.iso_iii
       FROM   db_general.tbldimpais x
       WHERE  x.Pais    LIKE CONCAT(raw.country, '%')
          OR  x.pais_in LIKE CONCAT(raw.country, '%')
          OR  x.pais_es LIKE CONCAT(raw.country, '%')
       LIMIT 1)                                          AS countrycode
    , COALESCE(UCASE(raw.country), 'OTHER')              AS country
    , IF(OCTET_LENGTH(raw.state) > 3
        , LCASE(raw.state), 'other')                     AS Estate
    , COALESCE(IF(OCTET_LENGTH(raw.city) > 3
        AND raw.city <> ''
        , LCASE(raw.city), 'other'), 'other')            AS ciudad
    , raw.address
    , raw.pcode                                          AS zip
    , COALESCE((SELECT CONCAT(xii.corplevel, 4)
                FROM   vtw.catcorpsdev xii
                WHERE  xii.idcorp IN (
                       SELECT x.idcorp
                       FROM   vtw.catdevelopers x
                       WHERE  x.iddev = raw.iddev)
                ), -1)                                   AS corpcode
    , COALESCE((SELECT xii.corpname
                FROM   vtw.catcorpsdev xii
                WHERE  xii.idcorp IN (
                       SELECT x.idcorp
                       FROM   vtw.catdevelopers x
                       WHERE  x.iddev = raw.iddev)
                ), 'INACTIVE')                           AS corp
    , 0                                                  AS ingreso
    , 0                                                  AS egreso
    , 5                                                  AS rank
    , raw.statusn                                        AS EstatusN
    , (SELECT x.statlet
       FROM   vtw.catstatus x
       WHERE  x.statusn = raw.statusn
       LIMIT 1)                                          AS EstatusL
    , CAST(raw.capdata AS DATE)                          AS createdAt
    , raw.updatedAt                                      AS updatedAt
    , raw.deletedAt                                      AS deletedAt
FROM   db_general.tbl_raw_clientsvc  raw
WHERE  raw.deletedAt IS NULL
  AND  raw.tradedid  <= {max_id}
  AND  raw.updatedAt >  {max_updatedat}
