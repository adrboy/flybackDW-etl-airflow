CREATE OR REPLACE PROCEDURE `update_flybackDW_tblInicioSolicitados_VI_hour`()
-- ============================================================
-- PROCEDURE : flybackDW.update_flybackDW_tblInicioSolicitados_VI_hour
-- OBJETIVO  : Sincronización incremental de tblInicioSolicitados
-- VERSIÓN   : 3.3 — 2026-09-02
-- CAMBIOS v3.3:
--   - Agregado paso de limpieza al final: DELETE de registros con
--     status_r = 0.00 (SIN USO) en origen. Eran redeems que el DAG
--     inserto cuando tenian status valido y luego fueron reseteados.
--     Se acumulaban silenciosamente causando diferencias en auditoria.
-- CAMBIOS v3.2:
--   - GROUP BY contador → GROUP BY B.indice
--     El alias 'contador' era ambiguo en MariaDB — tomaba referencia de
--     flybackDW.tblInicioSolicitados colapsando ~1,683 registros válidos
--     Con B.indice el CTE ve 2,031 registros vs 344 anteriores
-- CAMBIOS v3.1:
--   - Documentado: NOT ISNULL(B.fCorreo) excluye 1,277 redeems sin fecha
-- CAMBIOS v3.0:
--   - WHERE cambiado de MAX(contador)/MAX(Create_At) a LEFT JOIN
--     para detectar registros nuevos (T.contador IS NULL) y
--     modificados (DATE(B.updateAt) > DATE(T.Update_At))
--   - Update_At agregado al INSERT y ON DUPLICATE KEY UPDATE
-- ============================================================
BEGIN
    DECLARE v_audit_id BIGINT DEFAULT 0;
    DECLARE v_error    TEXT   DEFAULT NULL;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 v_error = MESSAGE_TEXT;
        UPDATE flybackDW.etl_audit_log
        SET    estado        = 'ERROR'
             , mensaje_error = v_error
             , fecha_fin     = NOW()
        WHERE  id = v_audit_id;
    END;

    -- ── Registro RUNNING ─────────────────────────────────
    INSERT INTO flybackDW.etl_audit_log
           (paquete, vista_origen, tabla_destino, max_id_inicio,
            filas_insertadas, tipo_ejecucion, estado, fecha_inicio)
    VALUES ('update_flybackDW_tblInicioSolicitados_VI_hour',
            'customers.redeems',
            'flybackDW.tblInicioSolicitados',
            (SELECT COALESCE(MAX(contador), 0) FROM flybackDW.tblInicioSolicitados),
            0, 'HORA', 'RUNNING', NOW());

    SET v_audit_id = LAST_INSERT_ID();

    -- ── INSERT principal ─────────────────────────────────
    INSERT
    INTO flybackDW.tblInicioSolicitados
           ( tipo, fecha, dia, mes, anio, AnioMes, NomMes, contador
           , clientid, pack, monto, currency
           , idcorp, iddev, status_r, nivel, Create_At, Update_At)
    WITH X AS (
        SELECT 'Solicitados'                                                        AS tipo
             , DATE(B.fCorreo)                                                      AS fecha
             , EXTRACT(DAY FROM B.fCorreo)                                          AS dia
             , MONTH(B.fCorreo)                                                     AS mes
             , YEAR(B.fCorreo)                                                      AS anio
             , EXTRACT(YEAR_MONTH FROM B.fCorreo)                                   AS AnioMes
             , ELT(MONTH(B.fCorreo)
                  ,'Enero','Febrero','Marzo','Abril','Mayo','Junio'
                  ,'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre') AS NomMes
             , B.indice                                                             AS contador
             , B.clientid                                                           AS clientid
             , IF(C.dppaidin = 2, 1, 0)                                            AS pack
             , COALESCE(B.monto_pagar, 0)                                          AS monto
             , COALESCE(B.currency, 'USD')                                         AS currency
             , (SELECT idcorp FROM customers.develops WHERE iddev = C.company)      AS idcorp
             , C.company                                                            AS iddev
             , B.status_r
             , 1                                                                    AS nivel
             , DATE(NOW())                                                          AS Create_At
             , DATE(B.updateAt)                                                     AS Update_At
        FROM   customers.redeems            B
        LEFT  JOIN customers.fb_clients     C  ON C.clientid = B.clientid
        LEFT  JOIN flybackDW.tblInicioSolicitados T ON T.contador = B.indice
        WHERE  NOT ISNULL(B.fCorreo)
        AND    IFNULL(B.status_r, 0) <> 0
        AND  (
                T.contador IS NULL
                OR DATE(COALESCE(B.updateAt, '2000-01-01')) >
                   DATE(COALESCE(T.Update_At, '2000-01-01'))
             )
        GROUP BY B.indice
    )
    SELECT tipo, fecha, dia, mes, anio, AnioMes, NomMes, contador
         , clientid, pack, monto, currency
         , idcorp, iddev, status_r, nivel, Create_At, Update_At
    FROM   X
    ON DUPLICATE KEY UPDATE
      fecha     = VALUES(fecha)
    , dia       = VALUES(dia)
    , mes       = VALUES(mes)
    , anio      = VALUES(anio)
    , AnioMes   = VALUES(AnioMes)
    , NomMes    = VALUES(NomMes)
    , clientid  = VALUES(clientid)
    , pack      = VALUES(pack)
    , monto     = VALUES(monto)
    , currency  = VALUES(currency)
    , idcorp    = VALUES(idcorp)
    , iddev     = VALUES(iddev)
    , status_r  = VALUES(status_r)
    , Update_At = VALUES(Update_At);

    -- ── Limpieza: eliminar registros SIN USO (status_r = 0.00) ──
    -- Registros que en algun momento tuvieron status valido, el DAG
    -- los inserto, y luego en onpremise fueron reseteados a 0.00.
    -- No deben permanecer en destino porque el filtro de origen
    -- excluye status_r = 0 (IFNULL(status_r,0) <> 0).
    DELETE t
    FROM flybackDW.tblInicioSolicitados t
    INNER JOIN customers.redeems r ON r.indice = t.contador
    WHERE r.status_r = 0.00;

    -- ── Registro OK ──────────────────────────────────────
    UPDATE flybackDW.etl_audit_log
    SET    filas_insertadas = ROW_COUNT()
         , estado           = 'OK'
         , fecha_fin        = NOW()
    WHERE  id = v_audit_id;

END
