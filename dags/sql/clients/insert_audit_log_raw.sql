-- ═══════════════════════════════════════════════════════
-- insert_audit_log_raw.sql
-- Objetivo : Registrar resultado de SP CDC en etl_audit_log
-- Uso      : etl_raw_da.registrar_log()
-- Nota     : {audit_tabla} se resuelve dinámicamente
--            242 → flybackDW.etl_audit_log
--            240 → db_general.etl_audit_log
-- ═══════════════════════════════════════════════════════
INSERT INTO {audit_tabla}
       ( paquete
       , vista_origen
       , tabla_destino
       , tipo_ejecucion
       , estado
       , mensaje_error
       , fecha_inicio
       , fecha_fin)
VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)
