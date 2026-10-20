-- ============================================================
-- SP CDC: etl_clientsvc   (archivo de despliegue)
-- Base      : db_general (instancia 240)
-- Ejecucion : manual en Navicat sobre MXPDCPROD240
--             (DROP + CREATE; MariaDB no tiene ALTER para el cuerpo)
-- Origen    : vtw.catowners + vtw.p_data
-- Destino   : db_general.tbl_raw_clientsvc
-- Version   : 1.1 - 2026-10-20
--             1.0: la que estaba desplegada (ocultaba los errores y
--                  permitia ejecuciones solapadas).
--             1.1: el handler relanza el error (RESIGNAL) para que
--                  Airflow lo vea como fallo, y candado GET_LOCK al
--                  inicio: una sola ejecucion a la vez (espera hasta
--                  300 s).
-- ============================================================

DROP PROCEDURE IF EXISTS `db_general`.`etl_clientsvc`;

DELIMITER $$

CREATE PROCEDURE `db_general`.`etl_clientsvc`()
BEGIN

    -- ── Variables generales ──────────────────────────────
    DECLARE v_audit_id     BIGINT   DEFAULT 0;
    DECLARE v_max_tradedid INT      DEFAULT 0;
    DECLARE v_filas        INT      DEFAULT 0;
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
        DO RELEASE_LOCK('db_general.etl_clientsvc');   -- libera el candado si lo tenia; si no, no hace nada
        RESIGNAL;   -- relanza el error original: Airflow lo ve como fallo
    END;

    -- ════════════════════════════════════════════════════
    -- BLOQUE 1 — INSERT nuevos clientes
    -- ════════════════════════════════════════════════════

    -- Candado: una sola ejecucion a la vez de este SP.
    -- Si otra corrida (o un CALL manual) lo tiene, espera hasta 300 s.
    -- Se pide ANTES de leer MAX(tradedid) para no duplicar llaves.
    IF COALESCE(GET_LOCK('db_general.etl_clientsvc', 300), 0) <> 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'etl_clientsvc sigue ocupado por otra ejecucion; reintentar';
    END IF;

    SET v_max_tradedid = COALESCE(
        (SELECT MAX(tradedid) FROM db_general.tbl_raw_clientsvc)
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
    VALUES ( 'etl_clientsvc'
           , 'vtw.catowners+p_data'
           , 'db_general.tbl_raw_clientsvc'
           , v_max_tradedid
           , 0
           , 'DIARIO-INSERT'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    INSERT INTO db_general.tbl_raw_clientsvc
           ( productid
           , tradedid
           , worldid
           , fname1
           , lname1
           , email1
           , email2
           , email3
           , ph11
           , ph12
           , ph13
           , capdata
           , country
           , state
           , city
           , address
           , pcode
           , iddev
           , statusn)
    SELECT   4
           , b.tradedid
           , a.worldid
           , a.fname1
           , a.lname1
           , a.email1
           , a.email2
           , a.email3
           , a.ph11
           , a.ph12
           , a.ph13
           , b.capdata
           , a.country
           , a.state
           , a.city
           , a.address
           , a.pcode
           , b.iddev
           , b.statusn
    FROM       vtw.catowners  a
    LEFT  JOIN vtw.p_data     b ON b.worldid  = a.worldid
    WHERE  b.tradedid IS NOT NULL
      AND  b.tradedid >  v_max_tradedid
    GROUP BY b.tradedid;

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
    VALUES ( 'etl_clientsvc'
           , 'vtw.catowners+p_data'
           , 'db_general.tbl_raw_clientsvc'
           , v_max_tradedid
           , 0
           , 'DIARIO-UPDATE'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    UPDATE db_general.tbl_raw_clientsvc        raw
    INNER JOIN (
        SELECT   b.tradedid
               , a.worldid
               , a.fname1
               , a.lname1
               , a.email1
               , a.email2
               , a.email3
               , a.ph11
               , a.ph12
               , a.ph13
               , b.capdata
               , a.country
               , a.state
               , a.city
               , a.address
               , a.pcode
               , b.iddev
               , b.statusn
               , GREATEST(
                   COALESCE(a.updatedAt, '2000-01-01')
                 , COALESCE(b.updatedAt, '2000-01-01')
                 )                              AS src_updatedAt
        FROM       vtw.catowners  a
        LEFT  JOIN vtw.p_data     b ON b.worldid = a.worldid
        WHERE  b.tradedid IS NOT NULL
        GROUP BY b.tradedid
    ) src ON src.tradedid = raw.tradedid
    SET    raw.worldid    = src.worldid
         , raw.fname1     = src.fname1
         , raw.lname1     = src.lname1
         , raw.email1     = src.email1
         , raw.email2     = src.email2
         , raw.email3     = src.email3
         , raw.ph11       = src.ph11
         , raw.ph12       = src.ph12
         , raw.ph13       = src.ph13
         , raw.capdata    = src.capdata
         , raw.country    = src.country
         , raw.state      = src.state
         , raw.city       = src.city
         , raw.address    = src.address
         , raw.pcode      = src.pcode
         , raw.iddev      = src.iddev
         , raw.statusn    = src.statusn
    WHERE (
            COALESCE(raw.fname1,   '') <> COALESCE(src.fname1,   '')
         OR COALESCE(raw.lname1,   '') <> COALESCE(src.lname1,   '')
         OR COALESCE(raw.email1,   '') <> COALESCE(src.email1,   '')
         OR COALESCE(raw.email2,   '') <> COALESCE(src.email2,   '')
         OR COALESCE(raw.email3,   '') <> COALESCE(src.email3,   '')
         OR COALESCE(raw.ph11,     '') <> COALESCE(src.ph11,     '')
         OR COALESCE(raw.ph12,     '') <> COALESCE(src.ph12,     '')
         OR COALESCE(raw.country,  '') <> COALESCE(src.country,  '')
         OR COALESCE(raw.state,    '') <> COALESCE(src.state,    '')
         OR COALESCE(raw.city,     '') <> COALESCE(src.city,     '')
         OR COALESCE(raw.address,  '') <> COALESCE(src.address,  '')
         OR COALESCE(raw.pcode,    '') <> COALESCE(src.pcode,    '')
         OR COALESCE(raw.iddev,    '') <> COALESCE(src.iddev,    '')
         OR COALESCE(raw.statusn,   0) <> COALESCE(src.statusn,  0)
            -- Red de seguridad: GREATEST de ambos updatedAt
         OR raw.updatedAt < src.src_updatedAt
          );

    SET v_filas = ROW_COUNT();

    UPDATE db_general.etl_audit_log
    SET    filas_insertadas = v_filas
         , estado           = 'OK'
         , fecha_fin        = NOW()
    WHERE  id = v_audit_id;

    DO RELEASE_LOCK('db_general.etl_clientsvc');

END$$

DELIMITER ;
