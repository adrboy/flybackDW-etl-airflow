-- ═══════════════════════════════════════════════════════
-- insert_audit_log_raw_phone.sql
-- Objetivo : Registrar resultado de SP RAW de telefonos en etl_audit_log
-- Uso      : etl_raw_phone_da.registrar_log()
-- Nota     : la tabla de auditoria se resuelve dinamicamente
--            (db_general.etl_audit_log en 242 y 240)
--            filas_insertadas guarda los telefonos nuevos de la corrida
--            max_id_inicio no se usa: RAW de telefonos no tiene id incremental
-- ═══════════════════════════════════════════════════════
INSERT INTO {audit_tabla}
       ( paquete
       , vista_origen
       , tabla_destino
       , tipo_ejecucion
       , estado
       , filas_insertadas
       , mensaje_error
       , fecha_inicio
       , fecha_fin)
VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s)
