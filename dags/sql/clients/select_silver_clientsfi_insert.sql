-- ═══════════════════════════════════════════════════════
-- select_silver_clientsfi_insert.sql
-- Origen  : db_general.tbl_raw_clientsfi (MariaDB 240)
-- Destino : source.clientsfi (SQL Server)
-- Proceso : Silver INSERT — registros nuevos
-- Filtro  : client_id > {max_id}
-- Versión : 1.0 — 2026-09-22
-- ═══════════════════════════════════════════════════════
SELECT
      raw.productid
    , raw.contract_id                                    AS contractid
    , raw.client_id                                      AS clientid
    , COALESCE(
          NULLIF(TRIM(raw.email), '')
        , CONCAT(raw.client_id, '@noreply.financiamiento')
      )                                                  AS email
    , CAST(raw.reg_date AS DATE)                         AS capdata
    , raw.fname1                                         AS FirstName
    , raw.lname1                                         AS LastName
    , CASE raw.country
          WHEN '01USA' THEN 'USA'
          WHEN '03MEX' THEN 'MEX'
          WHEN '02CAN' THEN 'CAN'
          ELSE (SELECT x.iso_iii
                FROM   db_general.tbldimpais x
                WHERE  x.pais_in LIKE CONCAT(raw.country, '%')
                   OR  x.pais_es LIKE CONCAT(raw.country, '%')
                LIMIT 1)
      END                                                AS countrycode
    , CASE raw.country
          WHEN '01USA' THEN 'UNITED STATES'
          WHEN '03MEX' THEN 'MEXICO'
          WHEN '02CAN' THEN 'CANADA'
          ELSE (SELECT x.pais_in
                FROM   db_general.tbldimpais x
                WHERE  x.pais_in LIKE CONCAT(raw.country, '%')
                   OR  x.pais_es LIKE CONCAT(raw.country, '%')
                LIMIT 1)
      END                                                AS country
    , IF(OCTET_LENGTH(raw.state) > 3
        , LCASE(raw.state), 'other')                     AS Estate
    , COALESCE(IF(OCTET_LENGTH(raw.city) > 3
        AND raw.city <> ''
        , LCASE(raw.city), 'other'), 'other')            AS ciudad
    , raw.address
    , raw.zip
    , COALESCE((SELECT CONCAT(x.idcorp, 3)
                FROM   financiamiento.cat_develops x
                WHERE  x.deve = raw.deve
                ), -1)                                   AS corpcode
    , COALESCE((SELECT xii.corpname
                FROM   financiamiento.cat_devcorps xii
                WHERE  xii.idcorp IN (
                       SELECT x.idcorp
                       FROM   financiamiento.cat_develops x
                       WHERE  x.deve = raw.deve)
                ), 'INACTIVE')                           AS corp
    , 0                                                  AS ingreso
    , 0                                                  AS egreso
    , 5                                                  AS rank
    , raw.status_c                                       AS EstatusN
    , COALESCE((SELECT x.name_cr
                FROM   financiamiento.cat_status_cr x
                WHERE  x.id_status_cr = raw.status_c
                ), 'NaN')                                AS EstatusL
    , CAST(raw.reg_date AS DATE)                         AS createdAt
    , raw.updatedAt                                      AS updatedAt
    , raw.deletedAt                                      AS deletedAt
FROM   db_general.tbl_raw_clientsfi  raw
WHERE  raw.deletedAt IS NULL
  AND  raw.client_id > {max_id}
