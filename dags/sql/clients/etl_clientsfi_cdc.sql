-- ============================================================
-- SP CDC: etl_clientsfi   (archivo de despliegue)
-- Base      : db_general (instancia 240)
-- Ejecucion : manual en Navicat sobre MXPDCPROD240
--             (DROP + CREATE; MariaDB no tiene ALTER para el cuerpo)
-- Version   : 1.1 - 2026-10-20
--             1.1: el handler relanza el error (RESIGNAL) para que
--                  Airflow lo vea como fallo. La 1.0 lo ocultaba:
--                  el SP anotaba ERROR en etl_audit_log y el DAG
--                  reportaba OK igual.
-- ============================================================

DROP PROCEDURE IF EXISTS `db_general`.`etl_clientsfi`;

DELIMITER $$

CREATE PROCEDURE `db_general`.`etl_clientsfi`()
BEGIN
-- ============================================================
-- SP CDC: etl_clientsfi
-- Base    : db_general (instancia 240)
-- Origen  : financiamiento.clients + credits + clients_email
-- Destino : db_general.tbl_raw_clientsfi
-- Log     : db_general.etl_audit_log
-- Proceso : DIARIO — INSERT nuevos + UPDATE modificados
-- Versión : 1.1 — 2026-10-20
-- ============================================================

    -- ── Variables generales ──────────────────────────────
    DECLARE v_audit_id      BIGINT   DEFAULT 0;
    DECLARE v_max_client_id INT      DEFAULT 0;
    DECLARE v_filas         INT      DEFAULT 0;
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
        RESIGNAL;   -- relanza el error original: Airflow lo ve como fallo
    END;

    -- ════════════════════════════════════════════════════
    -- BLOQUE 1 — INSERT nuevos clientes
    -- ════════════════════════════════════════════════════

    SET v_max_client_id = COALESCE((SELECT MAX(client_id) FROM db_general.tbl_raw_clientsfi), 0);

    INSERT INTO db_general.etl_audit_log
           ( paquete
           , vista_origen
           , tabla_destino
           , max_id_inicio
           , filas_insertadas
           , tipo_ejecucion
           , estado
           , fecha_inicio)
    VALUES ( 'etl_clientsfi'
           , 'financiamiento.clients+credits+clients_email'
           , 'db_general.tbl_raw_clientsfi'
           , v_max_client_id
           , 0
           , 'DIARIO-INSERT'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    INSERT INTO db_general.tbl_raw_clientsfi
           ( productid
           , client_id
           , contract_id
           , fname1
           , lname1
           , email
           , reg_date
           , country
           , state
           , city
           , address
           , zip
           , deve
           , status_c)
    SELECT   3
           , a.client_id
           , a.contract_id
           , a.fname1
           , a.lname1
           , LCASE(SUBSTRING_INDEX(GROUP_CONCAT(c.email ORDER BY c.email_id ASC SEPARATOR ','), ',', 1)) AS email
           , a.reg_date
           , a.country
           , a.state
           , a.city
           , a.address
           , a.zip
           , b.deve
           , b.status_c
    FROM       financiamiento.clients         a
    INNER JOIN financiamiento.credits         b ON b.client_id    = a.client_id
    INNER  JOIN financiamiento.clients_email  c ON c.client_id    = a.client_id
    WHERE  a.contract_id      IS NOT NULL
    AND  a.client_id        >  v_max_client_id
    GROUP BY a.client_id;

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
    VALUES ( 'etl_clientsfi'
           , 'financiamiento.clients+credits+clients_email'
           , 'db_general.tbl_raw_clientsfi'
           , v_max_client_id
           , 0
           , 'DIARIO-UPDATE'
           , 'RUNNING'
           , NOW());

    SET v_audit_id = LAST_INSERT_ID();

    UPDATE db_general.tbl_raw_clientsfi        raw
    INNER JOIN (
        SELECT   a.client_id
               , a.contract_id
               , a.fname1
               , a.lname1
               , LCASE(SUBSTRING_INDEX(GROUP_CONCAT(c.email ORDER BY c.email_id ASC SEPARATOR ','), ',', 1))  AS email
               , a.reg_date
               , a.country
               , a.state
               , a.city
               , a.address
               , a.zip
               , b.deve
               , b.status_c
               , GREATEST(
                   COALESCE(a.updateAt,  '2000-01-01')
                 , COALESCE(b.updateAt,  '2000-01-01')
                 )                                   AS src_updateAt
        FROM       financiamiento.clients        a
        INNER JOIN financiamiento.credits        b ON b.client_id  = a.client_id
        INNER  JOIN financiamiento.clients_email c ON c.client_id  = a.client_id
        WHERE  a.contract_id    IS NOT NULL
        GROUP BY a.client_id
    ) src ON src.client_id = raw.client_id
    SET    raw.contract_id = src.contract_id
         , raw.fname1      = src.fname1
         , raw.lname1      = src.lname1
         , raw.email       = src.email
         , raw.reg_date    = src.reg_date
         , raw.country     = src.country
         , raw.state       = src.state
         , raw.city        = src.city
         , raw.address     = src.address
         , raw.zip         = src.zip
         , raw.deve        = src.deve
         , raw.status_c    = src.status_c
    WHERE (
            COALESCE(raw.fname1,      '') <> COALESCE(src.fname1,      '')
         OR COALESCE(raw.lname1,      '') <> COALESCE(src.lname1,      '')
         OR COALESCE(raw.email,       '') <> COALESCE(src.email,       '')
         OR COALESCE(raw.country,     '') <> COALESCE(src.country,     '')
         OR COALESCE(raw.state,       '') <> COALESCE(src.state,       '')
         OR COALESCE(raw.city,        '') <> COALESCE(src.city,        '')
         OR COALESCE(raw.address,     '') <> COALESCE(src.address,     '')
         OR COALESCE(raw.zip,         '') <> COALESCE(src.zip,         '')
         OR COALESCE(raw.deve,        '') <> COALESCE(src.deve,        '')
         OR COALESCE(raw.status_c,    0)  <> COALESCE(src.status_c,   0)
            -- Red de seguridad: mayor updateAt de clients o credits
         OR raw.updatedAt < src.src_updateAt
          );

    SET v_filas = ROW_COUNT();

    UPDATE db_general.etl_audit_log
    SET    filas_insertadas = v_filas
         , estado           = 'OK'
         , fecha_fin        = NOW()
    WHERE  id = v_audit_id;

END$$

DELIMITER ;
