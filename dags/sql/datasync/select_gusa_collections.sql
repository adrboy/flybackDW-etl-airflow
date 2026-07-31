-- ═══════════════════════════════════════════════════════
-- select_gusa_collections.sql
-- Objetivo : Leer clientes desde financiamiento
-- Origen   : 192.168.10.240  (financiamiento)
-- Versión  : 2.0 — 2026-07-30
-- Cambios  : CTEs para pre-agregar credits_collec,
--            credits_collec_ta, clients_email, clients_phone
--            antes del JOIN — de 37 minutos a 3 segundos
-- ═══════════════════════════════════════════════════════
WITH cobranza AS (
    SELECT
          client_id
        , COUNT(DISTINCT idx)                                AS paid_number
        , IFNULL(SUM(CASE WHEN amprogmxp > 0
                          THEN IFNULL(pay_amount, 0)
                          ELSE pay_amount END), 0)           AS paid
    FROM financiamiento.credits_collec
    WHERE statusc IN (5,6,7,14,16)
    GROUP BY client_id
),
saldo AS (
    SELECT
          client_id
        , MAX(saldo_final)                                   AS balance
    FROM financiamiento.credits_collec_ta
    WHERE estatus = 1
    GROUP BY client_id
),
email AS (
    SELECT
          client_id
        , MAX(email)                                         AS email
        , MIN(email)                                         AS emailTwo
    FROM financiamiento.clients_email
    GROUP BY client_id
),
telefono AS (
    SELECT
          client_id
        , MAX(number)                                        AS telephone
    FROM financiamiento.clients_phone
    GROUP BY client_id
)
SELECT
      1                                                      AS update_id
    , IFNULL(UPPER(CONCAT(cl.fname1, ' ', cl.lname1)), '')  AS client
    , IFNULL(UPPER(CONCAT(cl.fname2, ' ', cl.lname2)), '')  AS clientTwo
    , IFNULL(UPPER(cl.fname1), '')                          AS fname1
    , IFNULL(UPPER(cl.lname1), '')                          AS lname1
    , IFNULL(UPPER(cl.fname2), '')                          AS fname2
    , IFNULL(UPPER(cl.lname2), '')                          AS lname2
    , (IF(SUBSTRING(cr.contract, 1, 2) = 'D-',
          SUBSTRING(cr.contract, 3),
          IF(SUBSTRING(cr.contract, LENGTH(cr.contract)) = 'R',
             SUBSTRING(cr.contract, 1, LENGTH(cr.contract) - 1),
             cr.contract)))                                  AS contract
    , IFNULL(UPPER(em.email),      '')                      AS email
    , IFNULL(UPPER(em.emailTwo),   '')                      AS emailTwo
    , IFNULL(UPPER(tel.telephone), '')                      AS telephone
    , 1                                                      AS gc
    , IFNULL((SELECT iso_iii FROM CatPaises.cat_pais
              WHERE Pais    = cl.country
                 OR pais_in = cl.country
                 OR pais_es = cl.country
              LIMIT 1), 'OTH')                               AS country_code
    , IFNULL(cl.country, '')                                 AS country
    , IFNULL(cl.state,   '')                                 AS state
    , cr.deve                                                AS dev
    , IFNULL(csdc.corpname, '')                              AS corp
    , cr.fin_amount                                          AS credit
    , cr.num_pay                                             AS pay_number
    , IFNULL(co.paid_number, 0)                              AS paid_number
    , IFNULL(co.paid,        0)                              AS paid
    , IFNULL(sa.balance,     0)                              AS balance
    , cr.fund_cur                                            AS currency
    , cr.sig_date                                            AS sign
    , IFNULL(cscr.name_cr,  '')                              AS status
FROM financiamiento.credits cr
LEFT JOIN financiamiento.clients       cl   ON cl.client_id      = cr.client_id
LEFT JOIN cobranza                     co   ON co.client_id      = cr.client_id
LEFT JOIN saldo                        sa   ON sa.client_id      = cr.client_id
LEFT JOIN email                        em   ON em.client_id      = cr.client_id
LEFT JOIN telefono                     tel  ON tel.client_id     = cr.client_id
LEFT JOIN financiamiento.cat_status_cr cscr ON cscr.id_status_cr = cr.status_c
LEFT JOIN financiamiento.cat_develops  csd  ON csd.deve          = cr.deve
LEFT JOIN financiamiento.cat_devcorps  csdc ON csdc.idCorp       = csd.idCorp
WHERE cr.contract IS NOT NULL
