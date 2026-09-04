-- ============================================================
-- AUDITORÍA: tblInicioSolicitados vs customers.redeems
-- OBJETIVO : Comparar COUNT por año/mes entre origen y destino
-- RESULTADO: diferencia = 0  → mes sincronizado
--            diferencia > 0  → pendientes en origen
--            diferencia < 0  → sobran en destino (investigar)
-- FECHA    : 2026-06-25
-- ACTUALIZADO: 2026-09-02
-- ============================================================
-- NOTA: El filtro IFNULL(status_r,0) <> 0 es el mismo del DAG.
--       Los negativos son registros reseteados a status_r = 0
--       que el DAG limpia automaticamente con DELETE status_r = 0.
-- ============================================================
SELECT origen.anio
     , origen.mes
     , origen.NomMes
     , origen.cant                                        AS cant_origen
     , COALESCE(destino.cant, 0)                         AS cant_destino
     , origen.cant - COALESCE(destino.cant, 0)           AS diferencia
FROM (
    SELECT YEAR(B.fCorreo)   AS anio
         , MONTH(B.fCorreo)  AS mes
         , ELT(MONTH(B.fCorreo)
              ,'Enero','Febrero','Marzo','Abril','Mayo','Junio'
              ,'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre') AS NomMes
         , COUNT(1) AS cant
    FROM   customers.redeems       B
    LEFT JOIN customers.fb_clients C ON C.clientid = B.clientid
    WHERE  NOT ISNULL(B.fCorreo)
    AND    IFNULL(B.status_r, 0) <> 0   -- mismo filtro del DAG
    AND    B.fCorreo < CURDATE()
    GROUP BY anio, mes, NomMes
) origen
LEFT JOIN (
    SELECT anio, mes, COUNT(1) AS cant
    FROM   flybackDW.tblInicioSolicitados
    WHERE  fecha < CURDATE()
    GROUP BY anio, mes
) destino ON destino.anio = origen.anio
         AND destino.mes  = origen.mes
WHERE (origen.cant - COALESCE(destino.cant, 0)) <> 0
ORDER BY origen.anio, origen.mes;
