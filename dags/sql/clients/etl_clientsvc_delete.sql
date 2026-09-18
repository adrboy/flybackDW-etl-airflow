-- ============================================================
-- SP CDC DELETE: etl_clientsvc_delete
-- Base      : db_general (instancia 240)
-- Origen    : vtw.catowners + vtw.p_data
-- Destino   : db_general.tbl_raw_clientsvc
-- Log       : db_general.etl_audit_log
-- Proceso   : MENSUAL — soft delete huérfanos
-- Nota      : PK es tradedid (no clientid)
-- Versión   : 1.0 — 2026-09-18
-- ============================================================

DROP PROCEDURE IF EXISTS `db_general`.`etl_clientsvc_delete`;

DELIMITER $$

CREATE PROCEDURE `db_general`.`etl_clientsvc_delete`()
BEGIN

    DECLARE v_audit_id  BIGINT  DEFAULT 0;
    DECLARE v_filas     INT     DEFAULT 0;
    DECLARE v_error     TEXT;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 v_error = MESSAGE_TEXT;
        UPDATE db_general.etl_audit_log
        SET    estado        = 'ERROR'
             , mensaje_error = v_error
             , fecha_fin     = NOW()
        WHERE  id = v_audit_id;
    END;

    -- ── Registro RUNNING ─────────────────────────────────
    INSERT INTO db_general.etl_audit_log
           ( paquete
           , vista_origen
           , tabla_destino
           , max_id_inicio
           , filas_insertadas
           , tipo_ejecucion
           , estado
           , fecha_inicio)
    VALUES ( 'etl_clientsvc_delete'
           , 'vtw.catowners+p_data'
           , 'db_general.tbl_raw_clientsvc'
           , (SELECT COALESCE(MAX(tradedid), 0)
              FROM   db_general.tbl_raw_clientsvc)
           , 0
           , 'VERIFY-DELETE'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    -- ── Soft delete huérfanos ────────────────────────────
    -- tradedid en RAW que ya no existe en p_data
    UPDATE db_general.tbl_raw_clientsvc  raw
    SET    raw.deletedAt = NOW()
    WHERE  raw.deletedAt IS NULL
      AND  NOT EXISTS (
               SELECT 1
               FROM       vtw.catowners  a
               LEFT  JOIN vtw.p_data     b ON b.worldid = a.worldid
               WHERE  b.tradedid IS NOT NULL
                 AND  b.tradedid = raw.tradedid
           );

    -- ── Cierre OK ────────────────────────────────────────
    SET v_filas = ROW_COUNT();

    UPDATE db_general.etl_audit_log
    SET    filas_insertadas = v_filas
         , estado           = 'OK'
         , fecha_fin        = NOW()
    WHERE  id = v_audit_id;

END$$

DELIMITER ;
