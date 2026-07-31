-- ═══════════════════════════════════════════════════════
-- insert_complete.sql
-- Objetivo : Consolidar gusa_collections + flyback +
--            buyback + vtw → db_general.complete
-- Servidor : 192.168.10.242  (todo interno en db_general)
-- Versión  : 1.0 — 2026-07-30
-- Nota     : Fiel al query original del C# GenerateData()
--            UNION de las 4 tablas intermedias ya cargadas
--            Depende de Fase 1 completa (las 4 tablas llenas)
-- ═══════════════════════════════════════════════════════
INSERT INTO db_general.complete (
      update_id
    , client,        clientTwo
    , fname1,        lname1,          fname2,          lname2
    , contract,      email,           emailTwo,        telephone
    , country_code
    , gc,            fb,              bb,              vtw
    , ml,            b,               pw,              rw,          total
    , gc_n_b,        gc_n_pw,         gc_n_rw
    , gc_country,    gc_state,        gc_dev,          gc_corp
    , gc_credit,     gc_pay_number,   gc_paid_number
    , gc_paid,       gc_balance,      gc_currency,     gc_sign,     gc_status
    , fb_ncert,      fb_vcert,        fb_sign,         fb_activated
    , fb_years,      fb_currency,     fb_status
    , fb_country,    fb_state,        fb_dev,          fb_corp
    , bb_ncert,      bb_vcert,        bb_sign,         bb_activated
    , bb_years,      bb_currency,     bb_status
    , bb_country,    bb_state,        bb_dev,          bb_corp
    , vtw_capdata,   vtw_fee,         vtw_status
    , vtw_country,   vtw_state,       vtw_dev,         vtw_corp
)
SELECT DISTINCT
      1                                                                     AS update_id
    , client
    , GROUP_CONCAT(DISTINCT IF(clientTwo = '', NULL, clientTwo))           AS clientTwo
    , fname1,  lname1,  fname2,  lname2
    , contract
    , MAX(email)                                                            AS email
    , MAX(emailTwo)                                                         AS emailTwo
    , MAX(telephone)                                                        AS telephone
    , GROUP_CONCAT(DISTINCT IF(country_code = 'OTH', NULL, country_code)) AS country_code
    , SUM(gc)                                                               AS gc
    , SUM(fb)                                                               AS fb
    , SUM(bb)                                                               AS bb
    , SUM(vtw)                                                              AS vtw
    , SUM(ml)                                                               AS ml
    , SUM(b)                                                                AS b
    , SUM(pw)                                                               AS pw
    , SUM(rw)                                                               AS rw
    , SUM(gc + fb + bb + vtw + ml + b + pw + rw)                           AS total
    , SUM(gc_n_b)                                                           AS gc_n_b
    , SUM(gc_n_pw)                                                          AS gc_n_pw
    , SUM(gc_n_rw)                                                          AS gc_n_rw
    , GROUP_CONCAT(gc_country)                                              AS gc_country
    , GROUP_CONCAT(gc_state)                                                AS gc_state
    , GROUP_CONCAT(gc_dev)                                                  AS gc_dev
    , GROUP_CONCAT(gc_corp)                                                 AS gc_corp
    , GROUP_CONCAT(gc_credit)                                               AS gc_credit
    , GROUP_CONCAT(gc_pay_number)                                           AS gc_pay_number
    , GROUP_CONCAT(gc_paid_number)                                          AS gc_paid_number
    , GROUP_CONCAT(gc_paid)                                                 AS gc_paid
    , GROUP_CONCAT(gc_balance)                                              AS gc_balance
    , GROUP_CONCAT(gc_currency)                                             AS gc_currency
    , GROUP_CONCAT(gc_sign)                                                 AS gc_sign
    , GROUP_CONCAT(gc_status)                                               AS gc_status
    , GROUP_CONCAT(fb_ncert)                                                AS fb_ncert
    , GROUP_CONCAT(fb_vcert)                                                AS fb_vcert
    , GROUP_CONCAT(fb_sign)                                                 AS fb_sign
    , GROUP_CONCAT(fb_activated)                                            AS fb_activated
    , GROUP_CONCAT(fb_years)                                                AS fb_years
    , GROUP_CONCAT(fb_currency)                                             AS fb_currency
    , GROUP_CONCAT(fb_status)                                               AS fb_status
    , GROUP_CONCAT(fb_country)                                              AS fb_country
    , GROUP_CONCAT(fb_state)                                                AS fb_state
    , GROUP_CONCAT(fb_dev)                                                  AS fb_dev
    , GROUP_CONCAT(fb_corp)                                                 AS fb_corp
    , GROUP_CONCAT(bb_ncert)                                                AS bb_ncert
    , GROUP_CONCAT(bb_vcert)                                                AS bb_vcert
    , GROUP_CONCAT(bb_sign)                                                 AS bb_sign
    , GROUP_CONCAT(bb_activated)                                            AS bb_activated
    , GROUP_CONCAT(bb_years)                                                AS bb_years
    , GROUP_CONCAT(bb_currency)                                             AS bb_currency
    , GROUP_CONCAT(bb_status)                                               AS bb_status
    , GROUP_CONCAT(bb_country)                                              AS bb_country
    , GROUP_CONCAT(bb_state)                                                AS bb_state
    , GROUP_CONCAT(bb_dev)                                                  AS bb_dev
    , GROUP_CONCAT(bb_corp)                                                 AS bb_corp
    , GROUP_CONCAT(vtw_capdata)                                             AS vtw_capdata
    , GROUP_CONCAT(vtw_fee)                                                 AS vtw_fee
    , GROUP_CONCAT(vtw_status)                                              AS vtw_status
    , GROUP_CONCAT(vtw_country)                                             AS vtw_country
    , GROUP_CONCAT(vtw_state)                                               AS vtw_state
    , GROUP_CONCAT(vtw_dev)                                                 AS vtw_dev
    , GROUP_CONCAT(vtw_corp)                                                AS vtw_corp
FROM (

    -- ── GC ───────────────────────────────────────────────────────────────
    SELECT
          gc.client,      gc.clientTwo
        , gc.fname1,      gc.lname1,      gc.fname2,    gc.lname2
        , gc.contract,    gc.email,       gc.emailTwo,  gc.telephone
        , gc.country_code
        , gc.gc,          0 fb,           0 bb,         0 vtw,        0 ml
        , IF(bpw.b  IS NULL, 0, IF(bpw.b  > 0, 1, 0))  b
        , IF(bpw.pw IS NULL, 0, IF(bpw.pw > 0, 1, 0))  pw
        , IF(bpw.rw IS NULL, 0, IF(bpw.rw > 0, 1, 0))  rw
        , 0 total
        , bpw.b  gc_n_b,  bpw.pw gc_n_pw, bpw.rw gc_n_rw
        , gc.country gc_country,  gc.state gc_state
        , gc.dev     gc_dev,      gc.corp  gc_corp
        , gc.credit  gc_credit,   gc.pay_number  gc_pay_number
        , gc.paid_number gc_paid_number
        , gc.paid    gc_paid,     gc.balance gc_balance
        , gc.currency gc_currency, gc.sign gc_sign, gc.status gc_status
        , NULL fb_ncert,    NULL fb_vcert,    NULL fb_sign,      NULL fb_activated
        , NULL fb_years,    NULL fb_currency, NULL fb_status
        , NULL fb_country,  NULL fb_state,    NULL fb_dev,       NULL fb_corp
        , NULL bb_ncert,    NULL bb_vcert,    NULL bb_sign,      NULL bb_activated
        , NULL bb_years,    NULL bb_currency, NULL bb_status
        , NULL bb_country,  NULL bb_state,    NULL bb_dev,       NULL bb_corp
        , NULL vtw_capdata, NULL vtw_fee,     NULL vtw_status
        , NULL vtw_country, NULL vtw_state,   NULL vtw_dev,      NULL vtw_corp
    FROM db_general.gusa_collections gc
    LEFT JOIN db_general.beyond_pw bpw
           ON gc.contract = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
              UPPER(bpw.contract), ')', ''), '(', ''), '-', ''), ' ', ''), '+', '')

    UNION

    -- ── FB ───────────────────────────────────────────────────────────────
    SELECT
          fb.client,      fb.clientTwo
        , fb.fname1,      fb.lname1,      fb.fname2,    fb.lname2
        , fb.contract,    fb.email,       fb.emailTwo,  fb.telephone
        , fb.country_code
        , 0 gc,           fb.fb,          0 bb,         0 vtw,        0 ml
        , 0 b,            0 pw,           0 rw,         0 total
        , 0 gc_n_b,       0 gc_n_pw,      0 gc_n_rw
        , NULL gc_country, NULL gc_state, NULL gc_dev,  NULL gc_corp
        , NULL gc_credit,  NULL gc_pay_number, NULL gc_paid_number
        , NULL gc_paid,    NULL gc_balance, NULL gc_currency
        , NULL gc_sign,    NULL gc_status
        , fb.ncert fb_ncert,      fb.vcert    fb_vcert
        , fb.sign  fb_sign,       fb.activated fb_activated
        , fb.years fb_years,      fb.currency  fb_currency,  fb.status fb_status
        , fb.country fb_country,  fb.state fb_state
        , fb.dev   fb_dev,        fb.corp  fb_corp
        , NULL bb_ncert,    NULL bb_vcert,    NULL bb_sign,      NULL bb_activated
        , NULL bb_years,    NULL bb_currency, NULL bb_status
        , NULL bb_country,  NULL bb_state,    NULL bb_dev,       NULL bb_corp
        , NULL vtw_capdata, NULL vtw_fee,     NULL vtw_status
        , NULL vtw_country, NULL vtw_state,   NULL vtw_dev,      NULL vtw_corp
    FROM db_general.flyback fb

    UNION

    -- ── BB ───────────────────────────────────────────────────────────────
    SELECT
          bb.client,      bb.clientTwo
        , bb.fname1,      bb.lname1,      bb.fname2,    bb.lname2
        , bb.contract,    bb.email,       bb.emailTwo,  bb.telephone
        , bb.country_code
        , 0 gc,           0 fb,           bb.bb,        0 vtw,        0 ml
        , 0 b,            0 pw,           0 rw,         0 total
        , 0 gc_n_b,       0 gc_n_pw,      0 gc_n_rw
        , NULL gc_country, NULL gc_state, NULL gc_dev,  NULL gc_corp
        , NULL gc_credit,  NULL gc_pay_number, NULL gc_paid_number
        , NULL gc_paid,    NULL gc_balance, NULL gc_currency
        , NULL gc_sign,    NULL gc_status
        , NULL fb_ncert,    NULL fb_vcert,    NULL fb_sign,      NULL fb_activated
        , NULL fb_years,    NULL fb_currency, NULL fb_status
        , NULL fb_country,  NULL fb_state,    NULL fb_dev,       NULL fb_corp
        , bb.ncert bb_ncert,      bb.vcert    bb_vcert
        , bb.sign  bb_sign,       bb.activated bb_activated
        , bb.years bb_years,      bb.currency  bb_currency,  bb.status bb_status
        , bb.country bb_country,  bb.state bb_state
        , bb.dev   bb_dev,        bb.corp  bb_corp
        , NULL vtw_capdata, NULL vtw_fee,     NULL vtw_status
        , NULL vtw_country, NULL vtw_state,   NULL vtw_dev,      NULL vtw_corp
    FROM db_general.buyback bb

    UNION

    -- ── VTW ──────────────────────────────────────────────────────────────
    SELECT
          vtw.client,     vtw.clientTwo
        , vtw.fname1,     vtw.lname1,     vtw.fname2,   vtw.lname2
        , vtw.contract,   vtw.email,      vtw.emailTwo, vtw.telephone
        , vtw.country_code
        , 0 gc,           0 fb,           0 bb,         vtw.vtw,      0 ml
        , 0 b,            0 pw,           0 rw,         0 total
        , 0 gc_n_b,       0 gc_n_pw,      0 gc_n_rw
        , NULL gc_country, NULL gc_state, NULL gc_dev,  NULL gc_corp
        , NULL gc_credit,  NULL gc_pay_number, NULL gc_paid_number
        , NULL gc_paid,    NULL gc_balance, NULL gc_currency
        , NULL gc_sign,    NULL gc_status
        , NULL fb_ncert,    NULL fb_vcert,    NULL fb_sign,      NULL fb_activated
        , NULL fb_years,    NULL fb_currency, NULL fb_status
        , NULL fb_country,  NULL fb_state,    NULL fb_dev,       NULL fb_corp
        , NULL bb_ncert,    NULL bb_vcert,    NULL bb_sign,      NULL bb_activated
        , NULL bb_years,    NULL bb_currency, NULL bb_status
        , NULL bb_country,  NULL bb_state,    NULL bb_dev,       NULL bb_corp
        , vtw.capdata vtw_capdata, vtw.fee vtw_fee,    vtw.status vtw_status
        , vtw.country vtw_country, vtw.state vtw_state
        , vtw.dev     vtw_dev,     vtw.corp  vtw_corp
    FROM db_general.vtw vtw

) t
GROUP BY  contract, client
ORDER BY  total DESC
