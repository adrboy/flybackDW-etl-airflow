-- ═══════════════════════════════════════════════════════
-- insert_gusa_collections.sql
-- Objetivo : Insertar un registro en db_general.gusa_collections
-- Destino  : 192.168.10.242  (db_general)
-- Versión  : 1.0 — 2026-07-30
-- Nota     : sign llega ya como None desde Python
--            cuando la fecha es inválida
-- ═══════════════════════════════════════════════════════
INSERT INTO db_general.gusa_collections (
      update_id, client,       clientTwo
    , fname1,    lname1,       fname2,      lname2
    , contract,  email,        emailTwo,    telephone
    , gc,        country_code, country,     state
    , dev,       corp
    , credit,    pay_number,   paid_number, paid
    , balance,   currency,     sign,        status
) VALUES (
      %s, %s, %s
    , %s, %s, %s, %s
    , %s, %s, %s, %s
    , %s, %s, %s, %s
    , %s, %s
    , %s, %s, %s, %s
    , %s, %s, %s, %s
)
