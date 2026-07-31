-- ═══════════════════════════════════════════════════════
-- insert_buyback.sql
-- Objetivo : Insertar un registro en db_general.buyback
-- Destino  : 192.168.10.242  (db_general)
-- Versión  : 1.1 — 2026-07-30
-- Nota     : sign y activated llegan ya como None desde Python
--            cuando la fecha es inválida (< 1000-01-01)
-- ═══════════════════════════════════════════════════════
INSERT INTO db_general.buyback (
      update_id, client,    clientTwo
    , fname1,    lname1,    fname2,    lname2
    , contract,  email,     emailTwo,  telephone
    , bb,        country_code
    , country,   state,     dev,       corp
    , ncert,     vcert
    , sign,      activated, years,     currency, status
) VALUES (
      %s, %s, %s
    , %s, %s, %s, %s
    , %s, %s, %s, %s
    , %s, %s
    , %s, %s, %s, %s
    , %s, %s
    , %s, %s
    , %s, %s, %s
)
