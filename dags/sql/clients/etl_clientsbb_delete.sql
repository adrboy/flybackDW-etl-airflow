-- ============================================================
-- SP CDC DELETE: etl_clientsbb_delete
-- Base      : db_general (instancia 242)
-- Origen    : buyback.clients
-- Destino   : db_general.tbl_raw_clientsbb
-- Log       : db_general.etl_audit_log
-- Proceso   : MENSUAL — soft delete huérfanos
-- Versión   : 1.0 — 2026-09-18
-- ============================================================

DROP PROCEDURE IF EXISTS `db_general`.`etl_clientsbb_delete`;

DELIMITER $$

CREATE PROCEDURE `db_general`.`etl_clientsbb_delete`()
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
    VALUES ( 'etl_clientsbb_delete'
           , 'buyback.clients'
           , 'db_general.tbl_raw_clientsbb'
           , (SELECT COALESCE(MAX(clientid), 0)
              FROM   db_general.tbl_raw_clientsbb)
           , 0
           , 'VERIFY-DELETE'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    -- ── Soft delete huérfanos ────────────────────────────
    UPDATE db_general.tbl_raw_clientsbb  raw
    SET    raw.deletedAt = NOW()
    WHERE  raw.deletedAt IS NULL
      AND  NOT EXISTS (
               SELECT 1
               FROM   buyback.clients  src
               WHERE  src.clientid   = raw.clientid
                 AND  src.contractid IS NOT NULL
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
