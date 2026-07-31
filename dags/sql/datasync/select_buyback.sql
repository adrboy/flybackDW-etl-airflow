-- ═══════════════════════════════════════════════════════
-- select_buyback.sql
-- Objetivo : Leer clientes desde buyback.clients
-- Origen   : 192.168.10.242  (buyback)
-- Versión  : 2.0 — 2026-07-30
-- Cambios  : Sin GROUP BY, sin COUNT(*), sin SUM —
--            buyback.clients es 1 a 1 por clientid
-- ═══════════════════════════════════════════════════════
SELECT
      1                                                                                AS update_id
    , IFNULL(UPPER(CONCAT(c.fname1, ' ', c.lname1)), '')                             AS client
    , IFNULL(UPPER(CONCAT(c.fname2, ' ', c.lname2)), '')                             AS clientTwo
    , IFNULL(UPPER(c.fname1), '')                                                     AS fname1
    , IFNULL(UPPER(c.lname1), '')                                                     AS lname1
    , IFNULL(UPPER(c.fname2), '')                                                     AS fname2
    , IFNULL(UPPER(c.lname2), '')                                                     AS lname2
    , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        UPPER(c.contractid), ')', ''), '(', ''), '-', ''), ' ', ''), '+', '')         AS contract
    , IFNULL(UPPER(IF(c.ema1 = '' OR c.ema1 IS NULL,
                      IF(c.ema2 = '' OR c.ema2 IS NULL, c.ema2, c.ema2),
                      c.ema1)), '')                                                    AS email
    , IFNULL(UPPER(IF(c.ema2 = '' OR c.ema2 IS NULL, c.ema2, c.ema2)), '')           AS emailTwo
    , IFNULL(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        UPPER(c.ph11), ')', ''), '(', ''), '-', ''), ' ', ''), '+', ''), '')          AS telephone
    , 1                                                                                AS bb
    , IFNULL((SELECT iso_iii FROM CatPaises.cat_pais
              WHERE Pais    = c.country
                 OR pais_in = c.country
                 OR pais_es = c.country
              LIMIT 1), 'OTH')                                                         AS country_code
    , IFNULL(c.country, '')                                                            AS country
    , IFNULL(c.state,   '')                                                            AS state
    , IFNULL(c.company, '')                                                            AS dev
    , IFNULL(crp.corpname, '')                                                         AS corp
    , 1                                                                                AS ncert
    , IFNULL(c.income,      0)                                                         AS vcert
    , IFNULL(c.signdate,    '0001-01-01')                                              AS sign
    , IFNULL(c.factivacion, '0001-01-01')                                              AS activated
    , IFNULL(c.period,      0)                                                         AS years
    , 'USD'                                                                            AS currency
    , IFNULL(sb.descrip,   '')                                                         AS status
FROM buyback.clients c
LEFT JOIN buyback.estatus  sb  ON sb.idstatus = c.status
LEFT JOIN buyback.develops dev ON c.company   = dev.iddev
LEFT JOIN buyback.devcorps crp ON dev.idcorp  = crp.corplevel
