-- ============================================================
-- AuditoriaIgualacionActivos.sql
-- Objetivo : Comparar onpremise vs flybackDW.tblActivosRedeemCorp
--            agrupado por año — validar sincronizacion CDC
-- Tabla    : flybackDW.tblActivosRedeemCorp
-- DAG      : flybackDW_sp_ActivosRedeemCorp (diario 6am)
-- SP       : flybackDW.sp_ActivosRedeemCorp()
-- Resultado: diferencias = 0 en todos los años → tabla sincronizada
-- REGLA -4 : statusf = -4 (finalizados) solo cuenta si
--            YEAR(inicio_rr) < YEAR(NOW()) — no el año actual
-- Creado   : 2026-09-14
-- Actualizado: 2026-09-15
-- ============================================================
-- NOTA: Se elimino el filtro redeem_no = 1 porque existen 64
--       clientes legitimos sin redeem_no = 1 pero con cobros
--       y pagados reales. Solo se valida redeem_no > 0.
-- ============================================================
WITH clientes AS (
    SELECT DISTINCT X.clientid
         , IF(X.dppaidin = 2, 1, 0)                                    AS pack
         , X.status                                                     AS statusf
         , YEAR(X.inicio_r)                                             AS anio
    FROM customers.fb_clients  X
    INNER JOIN customers.activos   XI   ON XI.clientid  = X.clientid
    INNER JOIN customers.redeems   XII  ON XII.clientid = X.clientid
                                       AND XII.complemento = 0
                                       AND XII.redeem_no   > 0
    WHERE (
        -- Año actual: solo vigentes
        (YEAR(X.inicio_r) = YEAR(NOW()) AND X.status > 3 AND X.status < 6)
        OR
        -- Años anteriores: vigentes + finalizados
        (YEAR(X.inicio_r) < YEAR(NOW()) AND ((X.status > 3 AND X.status < 6) OR X.status IN (-4)))
    )
    AND YEAR(X.inicio_r) BETWEEN 2013 AND YEAR(NOW())
)
, fuente AS (
    SELECT anio
         , SUM(IF(pack = 1, 1, 0)) AS CANTPACK
         , SUM(IF(pack = 0, 1, 0)) AS CANTNOPACK
    FROM clientes
    GROUP BY anio
)
, materializada AS (
    SELECT YEAR(inicio_rr)    AS anio
         , SUM(IF(pack = 1 AND (
               (statusf > 3 AND statusf < 6)
               OR (statusf IN (-4) AND YEAR(inicio_rr) < YEAR(NOW()))
           ), 1, 0))           AS CANTPACK
         , SUM(IF(pack = 0 AND (
               (statusf > 3 AND statusf < 6)
               OR (statusf IN (-4) AND YEAR(inicio_rr) < YEAR(NOW()))
           ), 1, 0))           AS CANTNOPACK
    FROM flybackDW.tblActivosRedeemCorp
    WHERE YEAR(inicio_rr) BETWEEN 2013 AND YEAR(NOW())
    GROUP BY YEAR(inicio_rr)
)
SELECT
      COALESCE(f.anio, m.anio)                                AS anio
    , COALESCE(f.CANTPACK,   0)                               AS fuente_PACK
    , COALESCE(m.CANTPACK,   0)                               AS dw_PACK
    , COALESCE(f.CANTPACK,   0) - COALESCE(m.CANTPACK,   0)  AS diferencias_PACK
    , COALESCE(f.CANTNOPACK, 0)                               AS fuente_NOPACK
    , COALESCE(m.CANTNOPACK, 0)                               AS dw_NOPACK
    , COALESCE(f.CANTNOPACK, 0) - COALESCE(m.CANTNOPACK, 0)  AS diferencias_NOPACK
FROM fuente f
LEFT JOIN materializada m ON m.anio = f.anio
ORDER BY anio;
