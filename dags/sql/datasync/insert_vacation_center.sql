-- ═══════════════════════════════════════════════════════
-- insert_vacation_center.sql
-- Objetivo : Insertar un registro en db_general.vtw
-- Destino  : 192.168.10.242  (db_general)
-- Versión  : 1.0 — 2026-07-30
-- Nota     : capdata llega ya como None desde Python
--            cuando la fecha es inválida (< 0001-01-01)
-- ═══════════════════════════════════════════════════════
INSERT INTO db_general.vtw (
      update_id, contract,     client,   clientTwo
    , fname1,    lname1,       fname2,   lname2
    , email,     emailTwo,     telephone
    , vtw,       country_code, country,  state
    , dev,       corp
    , capdata,   fee,          status
) VALUES (
      %s, %s, %s, %s
    , %s, %s, %s, %s
    , %s, %s, %s
    , %s, %s, %s, %s
    , %s, %s
    , %s, %s, %s
)
