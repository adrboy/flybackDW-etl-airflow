-- ═══════════════════════════════════════════════════════
-- select_flyback.sql
-- Objetivo : Leer clientes desde customers.fb_clients
-- Origen   : 192.168.10.242  (customers)
-- Versión  : 2.2 — 2026-07-31
-- Cambios  : Sin manejo de fechas — sign y activated
--            llegan como NULL natural desde la BD
-- ═══════════════════════════════════════════════════════
WITH corp AS (
    SELECT
          dv.iddev        AS iddev
        , dc.corpname     AS corpname
    FROM customers.develops   dv
    LEFT JOIN customers.devcorps dc ON dc.corplevel = dv.idcorp
),
tipo_cambio AS (
    SELECT
          anno
        , AVG(exchange_rate) AS avg_rate
    FROM customers.cat_date
    GROUP BY anno
),
redeem AS (
    SELECT
          contractid
        , MAX(redeem_no) AS years
    FROM customers.redeems
    GROUP BY contractid
)
SELECT
      1                                                                               AS update_id
    , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        UPPER(fb.contractid), ')', ''), '(', ''), '-', ''), ' ', ''), '+', '')       AS contract
    , IFNULL(UPPER(CONCAT(fb.fname1, ' ', fb.lname1)), '')                           AS client
    , IFNULL(UPPER(CONCAT(fb.fname2, ' ', fb.lname2)), '')                           AS clientTwo
    , IFNULL(UPPER(fb.fname1), '')                                                   AS fname1
    , IFNULL(UPPER(fb.lname1), '')                                                   AS lname1
    , IFNULL(UPPER(fb.fname2), '')                                                   AS fname2
    , IFNULL(UPPER(fb.lname2), '')                                                   AS lname2
    , IFNULL(UPPER(IF(fb.ema1 = '' OR fb.ema1 IS NULL,
                      IF(fb.ema2 = '' OR fb.ema2 IS NULL, fb.ema3, fb.ema2),
                      fb.ema1)), '')                                                  AS email
    , IFNULL(UPPER(IF(fb.ema2 = '' OR fb.ema2 IS NULL,
                      fb.ema3, fb.ema2)), '')                                         AS emailTwo
    , IFNULL(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        UPPER(fb.ph11), ')', ''), '(', ''), '-', ''), ' ', ''), '+', ''), '')        AS telephone
    , 1                                                                              AS fb
    , IFNULL((SELECT iso_iii FROM CatPaises.cat_pais
              WHERE Pais    = fb.country
                 OR pais_in = fb.country
                 OR pais_es = fb.country
              LIMIT 1), 'OTH')                                                       AS country_code
    , IFNULL(fb.country,  '')                                                        AS country
    , IFNULL(fb.state,    '')                                                        AS state
    , IFNULL(fb.company,  '')                                                        AS dev
    , IFNULL(cp.corpname, '')                                                        AS corp
    , 1                                                                              AS ncert
    , IFNULL(IF(fb.venta >= 9000,
                fb.venta / tc.avg_rate,
                fb.venta), 0)                                                        AS vcert
    , fb.signdate                                                                    AS sign
    , fb.inicio_r                                                                    AS activated
    , IFNULL(rd.years, 0)                                                            AS years
    , fb.currency                                                                    AS currency
    , IFNULL(e.descrip,   '')                                                        AS status
FROM customers.fb_clients fb
LEFT JOIN corp          cp ON cp.iddev      = fb.company
LEFT JOIN tipo_cambio   tc ON tc.anno       = YEAR(fb.capdata)
LEFT JOIN redeem        rd ON rd.contractid = fb.contractid
LEFT JOIN customers.estatus e ON e.idstatus = fb.status
WHERE NOT ISNULL(fb.contractid)
