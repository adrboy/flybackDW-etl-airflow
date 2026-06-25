INSERT INTO flybackDW.etl_audit_log
       ( paquete, vista_origen, tabla_destino
       , tipo_ejecucion, estado, mensaje_error
       , fecha_inicio, fecha_fin)
VALUES ( '{sp}'
       , '{vista_origen}'
       , '{tabla_destino}'
       , 'HORA', 'ERROR', '{error}'
       , NOW(), NOW())
