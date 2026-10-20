-- ============================================================
-- SP CDC: etl_clientsbb   (archivo de despliegue)
-- Base      : db_general (instancia 242)
-- Ejecucion : manual en Navicat sobre MXPDCPROD242
--             (DROP + CREATE; MariaDB no tiene ALTER para el cuerpo)
-- Origen    : buyback.clients
-- Destino   : db_general.tbl_raw_clientsbb
-- Log       : db_general.etl_audit_log
-- Version   : 1.1 - 2026-10-20
--             1.0: la que estaba desplegada (ocultaba los errores,
--                  permitia ejecuciones solapadas y escribia su auditoria
--                  en flybackDW.etl_audit_log).
--             1.1: el handler relanza el error (RESIGNAL) para que
--                  Airflow lo vea como fallo, candado GET_LOCK al inicio
--                  (una sola ejecucion a la vez; espera hasta 300 s) y
--                  la auditoria pasa a db_general.etl_audit_log.
-- ============================================================

DROP PROCEDURE IF EXISTS `db_general`.`etl_clientsbb`;

DELIMITER $$

CREATE PROCEDURE `db_general`.`etl_clientsbb`()
BEGIN

    -- ── Variables generales ──────────────────────────────
    DECLARE v_audit_id     BIGINT  DEFAULT 0;
    DECLARE v_max_clientid INT     DEFAULT 0;
    DECLARE v_filas        INT     DEFAULT 0;
    DECLARE v_error        TEXT;

    -- ── Handler de error ─────────────────────────────────
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 v_error = MESSAGE_TEXT;
        UPDATE db_general.etl_audit_log
        SET    estado        = 'ERROR'
             , mensaje_error = v_error
             , fecha_fin     = NOW()
        WHERE  id = v_audit_id;
        DO RELEASE_LOCK('db_general.etl_clientsbb');   -- libera el candado si lo tenia; si no, no hace nada
        RESIGNAL;   -- relanza el error original: Airflow lo ve como fallo
    END;

    -- ════════════════════════════════════════════════════
    -- BLOQUE 1 — INSERT nuevos clientes
    -- ════════════════════════════════════════════════════

    -- Candado: una sola ejecucion a la vez de este SP.
    -- Si otra corrida (o un CALL manual) lo tiene, espera hasta 300 s.
    -- Se pide ANTES de leer MAX(clientid) para no duplicar llaves.
    IF COALESCE(GET_LOCK('db_general.etl_clientsbb', 300), 0) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'etl_clientsbb sigue ocupado por otra ejecucion; reintentar';
    END IF;

    SET v_max_clientid = COALESCE(
        (SELECT MAX(clientid) FROM db_general.tbl_raw_clientsbb)
    , 0);

    INSERT INTO db_general.etl_audit_log
           ( paquete
           , vista_origen
           , tabla_destino
           , max_id_inicio
           , filas_insertadas
           , tipo_ejecucion
           , estado
           , fecha_inicio)
    VALUES ( 'etl_clientsbb'
           , 'buyback.clients'
           , 'db_general.tbl_raw_clientsbb'
           , v_max_clientid
           , 0
           , 'DIARIO-INSERT'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    INSERT INTO db_general.tbl_raw_clientsbb
           ( productid
           , clientid
           , contractid
           , fname1
           , lname1
           , ema1
           , ema2
           , ph11
           , ph12
           , capdata
           , country
           , state
           , city
           , zip
           , company
           , status)
    SELECT   2
           , src.clientid
           , src.contractid
           , src.fname1
           , src.lname1
           , src.ema1
           , src.ema2
           , src.ph11
           , src.ph12
           , src.capdata
           , src.country
           , src.state
           , src.city
           , src.zip
           , src.company
           , src.status
    FROM   buyback.clients src
    WHERE  src.contractid  IS NOT NULL
      AND  src.clientid    >  v_max_clientid;

    SET v_filas = ROW_COUNT();

    UPDATE db_general.etl_audit_log
    SET    filas_insertadas = v_filas
         , estado           = 'OK'
         , fecha_fin        = NOW()
    WHERE  id = v_audit_id;

    -- ════════════════════════════════════════════════════
    -- BLOQUE 2 — UPDATE clientes modificados
    -- ════════════════════════════════════════════════════

    INSERT INTO db_general.etl_audit_log
           ( paquete
           , vista_origen
           , tabla_destino
           , max_id_inicio
           , filas_insertadas
           , tipo_ejecucion
           , estado
           , fecha_inicio)
    VALUES ( 'etl_clientsbb'
           , 'buyback.clients'
           , 'db_general.tbl_raw_clientsbb'
           , v_max_clientid
           , 0
           , 'DIARIO-UPDATE'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    UPDATE db_general.tbl_raw_clientsbb  raw
    INNER JOIN buyback.clients           src
            ON src.clientid   = raw.clientid
    SET    raw.contractid = src.contractid
         , raw.fname1     = src.fname1
         , raw.lname1     = src.lname1
         , raw.ema1       = src.ema1
         , raw.ema2       = src.ema2
         , raw.ph11       = src.ph11
         , raw.ph12       = src.ph12
         , raw.capdata    = src.capdata
         , raw.country    = src.country
         , raw.state      = src.state
         , raw.city       = src.city
         , raw.zip        = src.zip
         , raw.company    = src.company
         , raw.status     = src.status
    WHERE  src.contractid  IS NOT NULL
      AND (
            COALESCE(raw.fname1,   '') <> COALESCE(src.fname1,   '')
         OR COALESCE(raw.lname1,   '') <> COALESCE(src.lname1,   '')
         OR COALESCE(raw.ema1,     '') <> COALESCE(src.ema1,     '')
         OR COALESCE(raw.ema2,     '') <> COALESCE(src.ema2,     '')
         OR COALESCE(raw.ph11,     '') <> COALESCE(src.ph11,     '')
         OR COALESCE(raw.ph12,     '') <> COALESCE(src.ph12,     '')
         OR COALESCE(raw.country,  '') <> COALESCE(src.country,  '')
         OR COALESCE(raw.state,    '') <> COALESCE(src.state,    '')
         OR COALESCE(raw.city,     '') <> COALESCE(src.city,     '')
         OR COALESCE(raw.zip,      '') <> COALESCE(src.zip,      '')
         OR COALESCE(raw.company,  '') <> COALESCE(src.company,  '')
         OR COALESCE(raw.status,   0)  <> COALESCE(src.status,   0)
         OR raw.updatedAt < src.updatedAt
          );

    SET v_filas = ROW_COUNT();

    UPDATE db_general.etl_audit_log
    SET    filas_insertadas = v_filas
         , estado           = 'OK'
         , fecha_fin        = NOW()
    WHERE  id = v_audit_id;

    DO RELEASE_LOCK('db_general.etl_clientsbb');

END$$

DELIMITER ;
