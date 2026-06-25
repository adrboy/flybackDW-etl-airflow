-- ============================================================
-- AUDITORÍA: tblInicioAutorizados vs customers.pago_redeem
-- OBJETIVO : Comparar COUNT por año/mes entre origen y destino
-- RESULTADO: diferencia = 0 → mes sincronizado
--            diferencia > 0 → pendientes en origen
--            diferencia < 0 → actualizaciones ON DUPLICATE KEY (normal)
-- FECHA    : 2026-06-25
-- ============================================================
SELECT origen.anio
     , origen.mes
     , origen.NomMes
     , origen.cant                       AS cant_origen
     , COALESCE(destino.cant, 0)         AS cant_destino
     , origen.cant - COALESCE(destino.cant, 0) AS diferencia
FROM (
    SELECT YEAR(A.f_authorized)  AS anio
         , MONTH(A.f_authorized) AS mes
         , ELT(MONTH(A.f_authorized)
              ,'Enero','Febrero','Marzo','Abril','Mayo','Junio'
              ,'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre') AS NomMes
         , COUNT(1) AS cant
    FROM   customers.pago_redeem          A
    INNER JOIN customers.redeems          B ON B.pagoid   = A.indice
    INNER JOIN customers.fb_clients       C ON C.clientid = B.clientid
    WHERE  NOT ISNULL(A.f_authorized)
    AND    A.f_authorized < CURDATE()
    GROUP BY anio, mes, NomMes
) origen
LEFT JOIN (
    SELECT anio, mes, COUNT(1) AS cant
    FROM   flybackDW.tblInicioAutorizados
    WHERE  fecha < CURDATE()
    GROUP BY anio, mes
) destino ON destino.anio = origen.anio
         AND destino.mes  = origen.mes
ORDER BY origen.anio
       , origen.mes;
