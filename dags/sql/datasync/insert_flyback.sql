-- ═══════════════════════════════════════════════════════
-- insert_flyback.sql
-- Objetivo : Insertar un registro en db_general.flyback
-- Destino  : 192.168.10.242  (db_general)
-- Versión  : 1.0 — 2026-07-30
-- Nota     : sign y activated llegan ya como None desde Python
--            cuando la fecha es inválida (< 0001-01-01)
-- ═══════════════════════════════════════════════════════
INSERT INTO db_general.flyback (
      update_id, contract,  client,   clientTwo
    , fname1,    lname1,    fname2,   lname2
    , email,     emailTwo,  telephone
    , fb,        country_code
    , country,   state,     dev,      corp
    , ncert,     vcert
    , sign,      activated, years,    currency, status
) VALUES (
      %s, %s, %s, %s
    , %s, %s, %s, %s
    , %s, %s, %s
    , %s, %s
    , %s, %s, %s, %s
    , %s, %s
    , %s, %s, %s, %s, %s
)
