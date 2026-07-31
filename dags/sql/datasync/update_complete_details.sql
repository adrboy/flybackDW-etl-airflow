-- ═══════════════════════════════════════════════════════
-- update_complete_details.sql
-- Objetivo : Guardar conteos de este run en complete_details
-- Tabla    : db_general.complete_details  (servidor 242)
-- Versión  : 1.0 — 2026-07-30
-- Parámetros: {update_id}, {gc}, {fb}, {bb}, {vtw}
-- ═══════════════════════════════════════════════════════
UPDATE db_general.complete_details
SET    gc  = {gc}
     , fb  = {fb}
     , bb  = {bb}
     , vtw = {vtw}
WHERE  update_id = {update_id}
