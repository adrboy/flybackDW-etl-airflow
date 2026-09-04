CREATE OR REPLACE PROCEDURE `update_flybackDW_tblInicioPagados_VI_hour`()
-- ============================================================
-- PROCEDURE : flybackDW.update_flybackDW_tblInicioPagados_VI_hour
-- OBJETIVO  : Sincronización incremental de tblInicioPagados
-- VERSIÓN   : 3.2 — 2026-09-04
-- CAMBIOS v3.2:
--   - PASO 0: INSERT faltantes — red de seguridad independiente de updateAt
--   - PASO 1: CDC UPDATE — actualiza columnas que cambiaron directamente
--   - PASO 2: Saneamiento — corrige dia/mes/anio desde fecha
--   - PASO 4: DELETE status_p < 3 SIEMPRE AL FINAL
-- CAMBIOS v3.1:
--   - GROUP BY pagoid → GROUP BY A.indice (alias ambiguo en MariaDB)
-- CAMBIOS v3.0:
--   - WHERE cambiado a LEFT JOIN, Update_At agregado al UPSERT
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
    INSERT
    INTO flybackDW.etl_audit_log
           ( paquete, vista_origen, tabla_destino, max_id_inicio
           , filas_insertadas, tipo_ejecucion, estado, fecha_inicio)
    VALUES ( 'update_flybackDW_tblInicioPagados_VI_hour'
           , 'customers.pago_redeem / redeems'
           , 'flybackDW.tblInicioPagados'
           , (SELECT COALESCE(MAX(pagoid), 0) FROM flybackDW.tblInicioPagados)
           , 0, 'HORA', 'RUNNING', NOW());

    SET v_audit_id = LAST_INSERT_ID();

    -- ── PASO 0: INSERT faltantes ─────────────────────────
    INSERT INTO flybackDW.tblInicioPagados
           (tipo, fecha, dia, mes, anio, AnioMes, NomMes, pagoid,
            clientid, pack, monto, currency,
            idcorp, iddev, status_p, nivel, Create_At, Update_At)
    SELECT 'Pagados'                                                             AS tipo
         , DATE(IFNULL(A.f_excel, A.f_pago))                                    AS fecha
         , EXTRACT(DAY   FROM IFNULL(A.f_excel, A.f_pago))                      AS dia
         , MONTH(IFNULL(A.f_excel, A.f_pago))                                   AS mes
         , YEAR(IFNULL(A.f_excel, A.f_pago))                                    AS anio
         , EXTRACT(YEAR_MONTH FROM IFNULL(A.f_excel, A.f_pago))                 AS AnioMes
         , ELT(MONTH(IFNULL(A.f_excel, A.f_pago))
              ,'Enero','Febrero','Marzo','Abril','Mayo','Junio'
              ,'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre')  AS NomMes
         , A.indice                                                              AS pagoid
         , B.clientid                                                            AS clientid
         , IF(C.dppaidin = 2, 1, 0)                                             AS pack
         , COALESCE(A.monto_pagar, 0)                                           AS monto
         , COALESCE(A.currency, 'USD')                                          AS currency
         , (SELECT idcorp FROM customers.develops WHERE iddev = C.company)      AS idcorp
         , C.company                                                             AS iddev
         , A.status_p
         , 3                                                                     AS nivel
         , DATE(NOW())                                                           AS Create_At
         , DATE(A.updateAt)                                                      AS Update_At
    FROM   customers.pago_redeem               A
    INNER JOIN customers.redeems               B ON B.pagoid   = A.indice
    LEFT  JOIN customers.fb_clients            C ON C.clientid = B.clientid
    WHERE  A.status_p >= 3
    AND    NOT ISNULL(IFNULL(A.f_excel, A.f_pago))
    AND    IFNULL(A.f_excel, A.f_pago) < CURDATE()
    AND    A.indice NOT IN (SELECT pagoid FROM flybackDW.tblInicioPagados)
    GROUP BY A.indice;

    -- ── PASO 1: CDC UPDATE ───────────────────────────────
    UPDATE flybackDW.tblInicioPagados          T
    INNER JOIN customers.pago_redeem           A ON A.indice   = T.pagoid
    INNER JOIN customers.redeems               B ON B.pagoid   = A.indice
    LEFT  JOIN customers.fb_clients            C ON C.clientid = B.clientid
    SET T.status_p  = A.status_p
      , T.monto     = COALESCE(A.monto_pagar, 0)
      , T.currency  = COALESCE(A.currency, 'USD')
      , T.pack      = IF(C.dppaidin = 2, 1, 0)
      , T.fecha     = DATE(IFNULL(A.f_excel, A.f_pago))
      , T.dia       = DAY(IFNULL(A.f_excel, A.f_pago))
      , T.mes       = MONTH(IFNULL(A.f_excel, A.f_pago))
      , T.anio      = YEAR(IFNULL(A.f_excel, A.f_pago))
      , T.AnioMes   = EXTRACT(YEAR_MONTH FROM IFNULL(A.f_excel, A.f_pago))
      , T.NomMes    = ELT(MONTH(IFNULL(A.f_excel, A.f_pago))
                         ,'Enero','Febrero','Marzo','Abril','Mayo','Junio'
                         ,'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre')
      , T.idcorp    = (SELECT idcorp FROM customers.develops WHERE iddev = C.company)
      , T.Update_At = DATE(NOW())
    WHERE T.status_p  <> A.status_p
       OR T.monto     <> COALESCE(A.monto_pagar, 0)
       OR T.currency  <> COALESCE(A.currency, 'USD')
       OR T.pack      <> IF(C.dppaidin = 2, 1, 0)
       OR T.fecha     <> DATE(IFNULL(A.f_excel, A.f_pago))
       OR T.idcorp    <> (SELECT idcorp FROM customers.develops WHERE iddev = C.company);

    -- ── PASO 2: Saneamiento ──────────────────────────────
    UPDATE flybackDW.tblInicioPagados
    SET    dia     = DAY(fecha)
         , mes     = MONTH(fecha)
         , anio    = YEAR(fecha)
         , AnioMes = YEAR(fecha) * 100 + MONTH(fecha)
         , NomMes  = ELT(MONTH(fecha)
                        ,'Enero','Febrero','Marzo','Abril','Mayo','Junio'
                        ,'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre')
    WHERE  dia  <> DAY(fecha)
        OR mes  <> MONTH(fecha)
        OR anio <> YEAR(fecha);

    -- ── PASO 3: UPSERT incremental ───────────────────────
    INSERT
    INTO flybackDW.tblInicioPagados
           ( tipo, fecha, dia, mes, anio, AnioMes, NomMes, pagoid
           , clientid, pack, monto, currency
           , idcorp, iddev, status_p, nivel, Create_At, Update_At)
    WITH X AS (
        SELECT 'Pagados'                                                            AS Tipo
             , DATE(IFNULL(A.f_excel, A.f_pago))                                   AS fecha
             , EXTRACT(DAY FROM IFNULL(A.f_excel, A.f_pago))                       AS Dia
             , MONTH(IFNULL(A.f_excel, A.f_pago))                                  AS mes
             , YEAR(IFNULL(A.f_excel, A.f_pago))                                   AS anio
             , EXTRACT(YEAR_MONTH FROM IFNULL(A.f_excel, A.f_pago))                AS AnioMes
             , ELT(MONTH(IFNULL(A.f_excel, A.f_pago))
                  ,'Enero','Febrero','Marzo','Abril','Mayo','Junio'
                  ,'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre') AS NomMes
             , A.indice                                                             AS pagoid
             , B.clientid                                                           AS clientid
             , IF(C.dppaidin = 2, 1, 0)                                            AS pack
             , COALESCE(A.monto_pagar, 0)                                          AS monto
             , COALESCE(A.currency, 'USD')                                         AS currency
             , (SELECT idcorp FROM customers.develops WHERE iddev = C.company)      AS idcorp
             , C.company                                                            AS iddev
             , A.status_p
             , 3                                                                    AS nivel
             , DATE(NOW())                                                         AS Create_At
             , DATE(A.updateAt)                                                    AS Update_At
        FROM   customers.pago_redeem          A
        INNER JOIN customers.redeems          B  ON B.pagoid   = A.indice
        LEFT  JOIN customers.fb_clients       C  ON C.clientid = B.clientid
        LEFT  JOIN flybackDW.tblInicioPagados T  ON T.pagoid   = A.indice
        WHERE  A.status_P >= 3
        AND    NOT ISNULL(IFNULL(A.f_excel, A.f_pago))
        AND  (
                T.pagoid IS NULL
                OR DATE(COALESCE(A.updateAt, '2000-01-01')) >
                   DATE(COALESCE(T.Update_At, '2000-01-01'))
             )
        GROUP BY A.indice
    )
    SELECT tipo, fecha, dia, mes, anio, AnioMes, NomMes, pagoid
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

    -- ── PASO 4: DELETE status_p < 3 ─────────────────────
    DELETE T
    FROM flybackDW.tblInicioPagados            T
    INNER JOIN customers.pago_redeem           R ON R.indice = T.pagoid
    WHERE R.status_p < 3;

    -- ── Registro OK ──────────────────────────────────────
    UPDATE flybackDW.etl_audit_log
    SET    filas_insertadas = ROW_COUNT()
         , estado           = 'OK'
         , fecha_fin        = NOW()
    WHERE  id = v_audit_id;

END
