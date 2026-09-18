-- ============================================================
-- SP CDC DELETE: etl_clientsfi_delete
-- Base      : db_general (instancia 240)
-- Origen    : financiamiento.clients + credits
-- Destino   : db_general.tbl_raw_clientsfi
-- Log       : db_general.etl_audit_log
-- Proceso   : MENSUAL — soft delete huérfanos
-- Nota      : PK es client_id (no clientid)
-- Versión   : 1.0 — 2026-09-18
-- ============================================================

DROP PROCEDURE IF EXISTS `db_general`.`etl_clientsfi_delete`;

DELIMITER $$

CREATE PROCEDURE `db_general`.`etl_clientsfi_delete`()
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
    VALUES ( 'etl_clientsfi_delete'
           , 'financiamiento.clients+credits'
           , 'db_general.tbl_raw_clientsfi'
           , (SELECT COALESCE(MAX(client_id), 0)
              FROM   db_general.tbl_raw_clientsfi)
           , 0
           , 'VERIFY-DELETE'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    -- ── Soft delete huérfanos ────────────────────────────
    -- cliente ya no existe en financiamiento.clients
    -- o ya no tiene crédito activo en credits
    UPDATE db_general.tbl_raw_clientsfi  raw
    SET    raw.deletedAt = NOW()
    WHERE  raw.deletedAt IS NULL
      AND  NOT EXISTS (
               SELECT 1
               FROM       financiamiento.clients  a
               INNER JOIN financiamiento.credits  b
                       ON b.client_id  = a.client_id
               WHERE  a.client_id    = raw.client_id
                 AND  a.contract_id  IS NOT NULL
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
