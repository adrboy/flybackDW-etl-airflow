-- ═══════════════════════════════════════════════════════
-- select_vacation_center.sql
-- Objetivo : Leer clientes desde vtw.p_data
-- Origen   : 192.168.10.240  (vtw)
-- Versión  : 2.0 — 2026-07-30
-- Cambios  : CTE fee para pre-agregar vtw.payments
--            Sin GROUP BY — p_data es 1 a 1 por tradedid
--            de 7.7 segundos a 4.1 segundos
-- ═══════════════════════════════════════════════════════
WITH fee AS (
    SELECT
          tradedid
        , COALESCE(SUM(monto), 0) AS fee
    FROM vtw.payments
    WHERE statusn IN (2,3,8)
    GROUP BY tradedid
)
SELECT
      1                                                                               AS update_id
    , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        UPPER(b.worldid), '.', ''), ')', ''), '(', ''), '-', ''), ' ', ''), '+', '') AS contract
    , COALESCE(UPPER(CONCAT(b.fname1, ' ', b.lname1)), '')                           AS client
    , COALESCE(UPPER(CONCAT(b.fname2, ' ', b.lname2)), '')                           AS clientTwo
    , COALESCE(UPPER(b.fname1), '')                                                  AS fname1
    , COALESCE(UPPER(b.lname1), '')                                                  AS lname1
    , COALESCE(UPPER(b.fname2), '')                                                  AS fname2
    , COALESCE(UPPER(b.lname2), '')                                                  AS lname2
    , COALESCE(UPPER(b.email1), '')                                                  AS email
    , COALESCE(UPPER(b.email2), '')                                                  AS emailTwo
    , COALESCE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        b.ph11, ')', ''), '(', ''), '-', ''), ' ', ''), '+', ''), '')               AS telephone
    , 1                                                                              AS vtw
    , IFNULL((SELECT iso_iii FROM CatPaises.cat_pais
              WHERE Pais    = b.country
                 OR pais_in = b.country
                 OR pais_es = b.country
              LIMIT 1), 'OTH')                                                       AS country_code
    , COALESCE(b.country, '')                                                        AS country
    , COALESCE(b.state,   '')                                                        AS state
    , COALESCE(a.iddev,   '')                                                        AS dev
    , COALESCE(crp.corpname, '')                                                     AS corp
    , COALESCE(DATE_FORMAT(a.capdata, '%Y-%m-%d'), '0001-01-01')                    AS capdata
    , COALESCE(f.fee, 0)                                                             AS fee
    , COALESCE(cs.stslet, '')                                                        AS status
FROM vtw.p_data a
LEFT JOIN  vtw.catowners        b   ON a.worldid   = b.worldid
INNER JOIN vtw.controlstatusvtw cs  ON a.statusn   = cs.statusn
                                   AND a.substatus  = cs.substatus
LEFT JOIN  vtw.catdevelopers    dev ON a.iddev      = dev.iddev
LEFT JOIN  vtw.catcorpsdev      crp ON dev.idcorp   = crp.idcorp
LEFT JOIN  fee                  f   ON f.tradedid   = a.tradedid
