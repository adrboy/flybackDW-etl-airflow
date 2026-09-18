-- ============================================================
-- SP CDC DELETE: etl_clientsml_delete
-- Base      : db_general (instancia 242)
-- Origen    : masterlinks.clients
-- Destino   : db_general.tbl_raw_clientsml
-- Log       : db_general.etl_audit_log
-- Proceso   : MENSUAL — soft delete huérfanos
-- Nota      : clientid <= 0 excluido (1 registro basura: -1)
-- Versión   : 1.0 — 2026-09-18
-- ============================================================

DROP PROCEDURE IF EXISTS `db_general`.`etl_clientsml_delete`;

DELIMITER $$

CREATE PROCEDURE `db_general`.`etl_clientsml_delete`()
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
    VALUES ( 'etl_clientsml_delete'
           , 'masterlinks.clients'
           , 'db_general.tbl_raw_clientsml'
           , (SELECT COALESCE(MAX(clientid), 0)
              FROM   db_general.tbl_raw_clientsml)
           , 0
           , 'VERIFY-DELETE'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    -- ── Soft delete huérfanos ────────────────────────────
    UPDATE db_general.tbl_raw_clientsml  raw
    SET    raw.deletedAt = NOW()
    WHERE  raw.deletedAt IS NULL
      AND  raw.clientid  > 0              -- excluye basura
      AND  NOT EXISTS (
               SELECT 1
               FROM   masterlinks.clients  src
               WHERE  src.clientid   = raw.clientid
                 AND  src.contractid IS NOT NULL
                 AND  src.clientid   > 0
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
