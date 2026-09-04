CREATE OR REPLACE PROCEDURE `update_flybackDW_tblInicioAutorizados_VI_hour`()
-- ============================================================
-- PROCEDURE : flybackDW.update_flybackDW_tblInicioAutorizados_VI_hour
-- OBJETIVO  : Sincronización incremental de tblInicioAutorizados
-- VERSIÓN   : 3.2 — 2026-09-03
-- CAMBIOS v3.2:
--   - PASO 0: INSERT faltantes — red de seguridad independiente
--     de updateAt. Inserta indices que no existen en destino.
--   - PASO 1: CDC UPDATE — actualiza columnas que cambiaron
--     comparando directamente origen vs destino sin depender solo
--     del updateAt. Si fecha cambia, actualiza tambien derivados.
--   - PASO 2: Saneamiento — corrige dia/mes/anio/NomMes/AnioMes
--     cuando no coinciden con fecha.
--   - PASO 4: DELETE al final — elimina status_p < 2 DESPUES
--     del UPSERT. Si el registro se reactiva el UPSERT lo recupera.
-- CAMBIOS v3.1:
--   - GROUP BY contador → GROUP BY A.indice
-- CAMBIOS v3.0:
--   - WHERE cambiado a LEFT JOIN para detectar nuevos y modificados
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
    VALUES ('update_flybackDW_tblInicioAutorizados_VI_hour',
            'customers.pago_redeem / redeems',
            'flybackDW.tblInicioAutorizados',
            (SELECT COALESCE(MAX(contador), 0) FROM flybackDW.tblInicioAutorizados),
            0, 'HORA', 'RUNNING', NOW());

    SET v_audit_id = LAST_INSERT_ID();

    -- ── PASO 0: INSERT faltantes ─────────────────────────
    INSERT INTO flybackDW.tblInicioAutorizados
        (tipo, fecha, dia, mes, anio, AnioMes, NomMes,
         contador, clientid, pack, monto, currency,
         idcorp, iddev, status_p, nivel, Create_At, Update_At)
    SELECT 'Autorizados'                                                         AS tipo
         , DATE(A.f_authorized)                                                  AS fecha
         , EXTRACT(DAY FROM A.f_authorized)                                      AS dia
         , MONTH(A.f_authorized)                                                 AS mes
         , YEAR(A.f_authorized)                                                  AS anio
         , EXTRACT(YEAR_MONTH FROM A.f_authorized)                               AS AnioMes
         , ELT(MONTH(A.f_authorized)
              ,'Enero','Febrero','Marzo','Abril','Mayo','Junio'
              ,'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre')   AS NomMes
         , A.indice                                                              AS contador
         , B.clientid                                                            AS clientid
         , IF(C.dppaidin = 2, 1, 0)                                             AS pack
         , COALESCE(A.monto_pagar, 0)                                            AS monto
         , COALESCE(A.currency, 'USD')                                           AS currency
         , (SELECT idcorp FROM customers.develops WHERE iddev = C.company)      AS idcorp
         , C.company                                                             AS iddev
         , A.status_p
         , 2                                                                     AS nivel
         , DATE(NOW())                                                           AS Create_At
         , DATE(A.updateAt)                                                      AS Update_At
    FROM   customers.pago_redeem               A
    INNER JOIN customers.redeems               B ON B.pagoid   = A.indice
    INNER JOIN customers.fb_clients            C ON C.clientid = B.clientid
    WHERE  NOT ISNULL(A.f_authorized)
    AND    A.f_authorized < CURDATE()
    AND    A.status_p >= 2
    AND    A.indice NOT IN (SELECT contador FROM flybackDW.tblInicioAutorizados)
    GROUP BY A.indice;

    -- ── PASO 1: CDC UPDATE ───────────────────────────────
    UPDATE flybackDW.tblInicioAutorizados t
    INNER JOIN customers.pago_redeem    A ON A.indice   = t.contador
    INNER JOIN customers.redeems        B ON B.pagoid   = A.indice
    INNER JOIN customers.fb_clients     C ON C.clientid = B.clientid
    SET t.status_p  = A.status_p
      , t.monto     = COALESCE(A.monto_pagar, 0)
      , t.currency  = COALESCE(A.currency, 'USD')
      , t.pack      = IF(C.dppaidin = 2, 1, 0)
      , t.fecha     = DATE(A.f_authorized)
      , t.dia       = DAY(A.f_authorized)
      , t.mes       = MONTH(A.f_authorized)
      , t.anio      = YEAR(A.f_authorized)
      , t.AnioMes   = EXTRACT(YEAR_MONTH FROM A.f_authorized)
      , t.NomMes    = ELT(MONTH(A.f_authorized)
                         ,'Enero','Febrero','Marzo','Abril','Mayo','Junio'
                         ,'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre')
      , t.idcorp    = (SELECT idcorp FROM customers.develops WHERE iddev = C.company)
      , t.Update_At = DATE(NOW())
    WHERE t.status_p  <> A.status_p
       OR t.monto     <> COALESCE(A.monto_pagar, 0)
       OR t.currency  <> COALESCE(A.currency, 'USD')
       OR t.pack      <> IF(C.dppaidin = 2, 1, 0)
       OR t.fecha     <> DATE(A.f_authorized)
       OR t.idcorp    <> (SELECT idcorp FROM customers.develops WHERE iddev = C.company);

    -- ── PASO 2: Saneamiento ──────────────────────────────
    UPDATE flybackDW.tblInicioAutorizados
    SET    dia    = DAY(fecha)
         , mes    = MONTH(fecha)
         , anio   = YEAR(fecha)
         , AnioMes = YEAR(fecha) * 100 + MONTH(fecha)
         , NomMes = ELT(MONTH(fecha)
                       ,'Enero','Febrero','Marzo','Abril','Mayo','Junio'
                       ,'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre')
    WHERE  dia  <> DAY(fecha)
        OR mes  <> MONTH(fecha)
        OR anio <> YEAR(fecha);

    -- ── PASO 3: INSERT / UPSERT ──────────────────────────
    INSERT
    INTO flybackDW.tblInicioAutorizados
           ( tipo, fecha, dia, mes, anio, AnioMes, NomMes, contador
           , clientid, pack, monto, currency
           , idcorp, iddev, status_p, nivel, Create_At, Update_At)
    WITH X AS (
        SELECT 'Autorizados'                                                        AS tipo
             , DATE(A.f_authorized)                                                 AS fecha
             , EXTRACT(DAY FROM A.f_authorized)                                     AS dia
             , MONTH(A.f_authorized)                                                AS mes
             , YEAR(A.f_authorized)                                                 AS anio
             , EXTRACT(YEAR_MONTH FROM A.f_authorized)                              AS AnioMes
             , ELT(MONTH(A.f_authorized)
                  ,'Enero','Febrero','Marzo','Abril','Mayo','Junio'
                  ,'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre') AS NomMes
             , A.indice                                                             AS contador
             , B.clientid                                                           AS clientid
             , IF(C.dppaidin = 2, 1, 0)                                            AS pack
             , COALESCE(A.monto_pagar, 0)                                          AS monto
             , COALESCE(A.currency, 'USD')                                         AS currency
             , (SELECT idcorp FROM customers.develops WHERE iddev = C.company)     AS idcorp
             , C.company                                                            AS iddev
             , A.status_p
             , 2                                                                    AS nivel
             , DATE(NOW())                                                          AS Create_At
             , DATE(A.updateAt)                                                     AS Update_At
        FROM   customers.pago_redeem          A
        INNER JOIN customers.redeems          B  ON B.pagoid   = A.indice
        INNER JOIN customers.fb_clients       C  ON C.clientid = B.clientid
        LEFT  JOIN flybackDW.tblInicioAutorizados T ON T.contador = A.indice
        WHERE  NOT ISNULL(A.f_authorized)
        AND  (
                T.contador IS NULL
                OR DATE(COALESCE(A.updateAt, '2000-01-01')) >
                   DATE(COALESCE(T.Update_At, '2000-01-01'))
             )
        GROUP BY A.indice
    )
    SELECT tipo, fecha, dia, mes, anio, AnioMes, NomMes, contador
         , clientid, pack, monto, currency
         , idcorp, iddev, status_p, nivel, Create_At, Update_At
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
    , status_p  = VALUES(status_p)
    , Update_At = VALUES(Update_At);

    -- ── PASO 4: DELETE status_p < 2 ─────────────────────
    DELETE t
    FROM flybackDW.tblInicioAutorizados t
    INNER JOIN customers.pago_redeem r ON r.indice = t.contador
    WHERE r.status_p < 2;

    -- ── Registro OK ──────────────────────────────────────
    UPDATE flybackDW.etl_audit_log
    SET    filas_insertadas = ROW_COUNT()
         , estado           = 'OK'
         , fecha_fin        = NOW()
    WHERE  id = v_audit_id;

END
