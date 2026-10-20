-- ============================================================
-- SP CDC: etl_clientsfb   (archivo de despliegue)
-- Base      : db_general (instancia 242)
-- Ejecucion : manual en Navicat sobre MXPDCPROD242
--             (DROP + CREATE; MariaDB no tiene ALTER para el cuerpo)
-- Origen    : customers.fb_clients
-- Destino   : db_general.tbl_raw_clientsfb
-- Log       : db_general.etl_audit_log   (igual que fi y vc)
-- Version   : 1.1 - 2026-10-20
--             1.0: la que estaba desplegada (ocultaba los errores y
--                  permitia ejecuciones solapadas).
--             1.1: el handler relanza el error (RESIGNAL) para que
--                  Airflow lo vea como fallo, y candado GET_LOCK al
--                  inicio: una sola ejecucion a la vez (espera hasta
--                  300 s).
--                  Ademas la auditoria pasa de flybackDW.etl_audit_log a
--                  db_general.etl_audit_log: el log de este proyecto vive
--                  en db_general (igual que fi, vc y el DAG).
-- ============================================================

DROP PROCEDURE IF EXISTS `db_general`.`etl_clientsfb`;

DELIMITER $$

CREATE PROCEDURE `db_general`.`etl_clientsfb`()
BEGIN
-- ============================================================
-- SP CDC: etl_clientsfb
-- Base    : db_general
-- Origen  : customers.fb_clients
-- Destino : db_general.tbl_raw_clientsfb
-- Log     : db_general.etl_audit_log
-- Proceso : DIARIO — INSERT nuevos + UPDATE modificados
-- Versión : 1.1 — 2026-10-20
-- ============================================================

    -- ── Variables generales ──────────────────────────────
    DECLARE v_audit_id      BIGINT  DEFAULT 0;
    DECLARE v_max_clientid  INT     DEFAULT 0;
    DECLARE v_filas         INT     DEFAULT 0;
    DECLARE v_error         TEXT;

    -- ── Handler de error ─────────────────────────────────
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 v_error = MESSAGE_TEXT;
        UPDATE db_general.etl_audit_log
        SET    estado        = 'ERROR'
             , mensaje_error = v_error
             , fecha_fin     = NOW()
        WHERE  id = v_audit_id;
        DO RELEASE_LOCK('db_general.etl_clientsfb');   -- libera el candado si lo tenia; si no, no hace nada
        RESIGNAL;   -- relanza el error original: Airflow lo ve como fallo
    END;

    -- ════════════════════════════════════════════════════
    -- BLOQUE 1 — INSERT nuevos clientes
    -- ════════════════════════════════════════════════════

    -- Candado: una sola ejecucion a la vez de este SP.
    -- Si otra corrida (o un CALL manual) lo tiene, espera hasta 300 s.
    -- Se pide ANTES de leer MAX(clientid) para no duplicar llaves.
    IF COALESCE(GET_LOCK('db_general.etl_clientsfb', 300), 0) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'etl_clientsfb sigue ocupado por otra ejecucion; reintentar';
    END IF;

    -- Watermark: máximo clientid actual en RAW
    SET v_max_clientid = COALESCE(
        (SELECT MAX(clientid) FROM db_general.tbl_raw_clientsfb)
    , 0);

    -- Registro RUNNING
    INSERT INTO db_general.etl_audit_log
           ( paquete
           , vista_origen
           , tabla_destino
           , max_id_inicio
           , filas_insertadas
           , tipo_ejecucion
           , estado
           , fecha_inicio)
    VALUES ( 'etl_clientsfb'
           , 'customers.fb_clients'
           , 'db_general.tbl_raw_clientsfb'
           , v_max_clientid
           , 0
           , 'DIARIO-INSERT'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    -- INSERT nuevos
    INSERT INTO db_general.tbl_raw_clientsfb
           ( productid
           , clientid
           , contractid
           , fname1
           , lname1
           , ema1
           , ema2
           , ema3
           , ph11
           , ph12
           , ph13
           , capdata
           , country
           , state
           , city
           , address
           , zip
           , company
           , status)
    SELECT   1
           , src.clientid
           , src.contractid
           , src.fname1
           , src.lname1
           , src.ema1
           , src.ema2
           , src.ema3
           , src.ph11
           , src.ph12
           , src.ph13
           , src.capdata
           , src.country
           , src.state
           , src.city
           , src.address
           , src.zip
           , src.company
           , src.status
    FROM   customers.fb_clients src
    WHERE  src.contractid IS NOT NULL
      AND  src.clientid   >  v_max_clientid;

    -- Cierre OK INSERT
    SET v_filas = ROW_COUNT();

    UPDATE db_general.etl_audit_log
    SET    filas_insertadas = v_filas
         , estado           = 'OK'
         , fecha_fin        = NOW()
    WHERE  id = v_audit_id;

    -- ════════════════════════════════════════════════════
    -- BLOQUE 2 — UPDATE clientes modificados
    -- ════════════════════════════════════════════════════

    -- Registro RUNNING
    INSERT INTO db_general.etl_audit_log
           ( paquete
           , vista_origen
           , tabla_destino
           , max_id_inicio
           , filas_insertadas
           , tipo_ejecucion
           , estado
           , fecha_inicio)
    VALUES ( 'etl_clientsfb'
           , 'customers.fb_clients'
           , 'db_general.tbl_raw_clientsfb'
           , v_max_clientid
           , 0
           , 'DIARIO-UPDATE'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    -- UPDATE por diferencia de columnas o updatedAt
    UPDATE db_general.tbl_raw_clientsfb  raw
    INNER JOIN customers.fb_clients      src
            ON src.clientid    = raw.clientid
    SET    raw.contractid  = src.contractid
         , raw.fname1      = src.fname1
         , raw.lname1      = src.lname1
         , raw.ema1        = src.ema1
         , raw.ema2        = src.ema2
         , raw.ema3        = src.ema3
         , raw.ph11        = src.ph11
         , raw.ph12        = src.ph12
         , raw.ph13        = src.ph13
         , raw.capdata     = src.capdata
         , raw.country     = src.country
         , raw.state       = src.state
         , raw.city        = src.city
         , raw.address     = src.address
         , raw.zip         = src.zip
         , raw.company     = src.company
         , raw.status      = src.status
    WHERE  src.contractid  IS NOT NULL
      AND (
            -- Principal: comparación directa de columnas
            COALESCE(raw.fname1,   '') <> COALESCE(src.fname1,   '')
         OR COALESCE(raw.lname1,   '') <> COALESCE(src.lname1,   '')
         OR COALESCE(raw.ema1,     '') <> COALESCE(src.ema1,     '')
         OR COALESCE(raw.ema2,     '') <> COALESCE(src.ema2,     '')
         OR COALESCE(raw.ema3,     '') <> COALESCE(src.ema3,     '')
         OR COALESCE(raw.ph11,     '') <> COALESCE(src.ph11,     '')
         OR COALESCE(raw.ph12,     '') <> COALESCE(src.ph12,     '')
         OR COALESCE(raw.country,  '') <> COALESCE(src.country,  '')
         OR COALESCE(raw.state,    '') <> COALESCE(src.state,    '')
         OR COALESCE(raw.city,     '') <> COALESCE(src.city,     '')
         OR COALESCE(raw.address,  '') <> COALESCE(src.address,  '')
         OR COALESCE(raw.zip,      '') <> COALESCE(src.zip,      '')
         OR COALESCE(raw.company,  '') <> COALESCE(src.company,  '')
         OR COALESCE(raw.status,   0)  <> COALESCE(src.status,   0)
            -- Red de seguridad: updatedAt origen más reciente que RAW
         OR raw.updatedAt < src.updatedAt
          );

    -- Cierre OK UPDATE
    SET v_filas = ROW_COUNT();

    UPDATE db_general.etl_audit_log
    SET    filas_insertadas = v_filas
         , estado           = 'OK'
         , fecha_fin        = NOW()
    WHERE  id = v_audit_id;

    DO RELEASE_LOCK('db_general.etl_clientsfb');

END$$

DELIMITER ;
