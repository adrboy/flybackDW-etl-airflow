-- ============================================================
-- SP: dw_etl.sp_upsert_factClientes   (archivo de despliegue)
-- Base      : DBGeneralDW (SQL Server 244)
-- Ejecucion : manual en SSMS (CREATE OR ALTER)
-- Lo llama  : dw_etl.sp_etl_maestro (paso 1 de 6)
-- Origen    : source.clientescorreo (sinonimo de source.vw_clientescorreo)
--             + cat.dimproducts
-- Destino   : gral.factClientes (un registro por correo)
-- Version   : 2.2 - 2026-10-21
--             2.0: cuerpo de sp_upsert_factClientes_v2 (2026-10-08), puesto
--                  bajo el nombre sp_upsert_factClientes el 2026-10-16 para
--                  que el maestro no cambie.
--             2.1: la auditoria se registra como 'sp_upsert_factClientes'.
--                  La v2 original la registraba como '..._v2', y el maestro
--                  (v1.1) busca el nombre sin sufijo.
--             2.2: capdata se conserva siempre en su fecha MAS ANTIGUA: se
--                  actualiza cuando la de la vista es menor y nunca se sube.
--                  (Antes solo se actualizaba producto: 2,855 correos tenian una
--                  fecha mas reciente que la minima de la vista; 2,614 de ellos
--                  incluyen VC. La primera corrida los corrige.)
-- Contrato con el maestro: deja en dw_etl_audit_log una fila de ESTA
--             corrida con estado 'Success' o 'Error'.
-- ============================================================

USE [DBGeneralDW]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dw_etl].[sp_upsert_factClientes]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @inicio                 DATETIME     = GETDATE()
    DECLARE @registros_insertados   INT          = 0
    DECLARE @registros_actualizados INT          = 0
    DECLARE @registros_procesados   INT          = 0
    DECLARE @estado                 NVARCHAR(50) = 'Success'
    DECLARE @descripcion            NVARCHAR(500)= ''
    DECLARE @tiempo_segundos        DECIMAL(10,2)

    DECLARE @MergeStats TABLE ([Action] NVARCHAR(10))

    BEGIN TRY

        MERGE gral.factClientes AS target
        USING (
            -- ── Agrupar por email + productid primero
            -- para eliminar repetición de productos
            SELECT
                  LOWER(ec.email)                        AS correo
                , MIN(ec.capdata)                        AS capdata
                , STRING_AGG(dp.shortName, ',')
                  WITHIN GROUP (ORDER BY ec.productid)   AS producto
            FROM (
                SELECT
                      LOWER(email)              AS email
                    , productid
                    , MIN(capdata)              AS capdata
                FROM   source.clientescorreo
                GROUP BY LOWER(email), productid
            ) ec
            INNER JOIN cat.dimproducts dp
                    ON dp.productid = ec.productid
            GROUP BY LOWER(ec.email)
        ) AS source
        ON target.correo = source.correo

        -- ── UPDATE si cambian los productos, o si la vista trae una fecha
        --    mas antigua (capdata se conserva siempre en su minimo)
        WHEN MATCHED AND (
                 target.producto COLLATE Latin1_General_CS_AS
                     <> source.producto  COLLATE Latin1_General_CS_AS
              OR source.capdata < ISNULL(target.capdata, '9999-12-31')
             ) THEN
            UPDATE SET
                  target.producto  = source.producto
                , target.capdata   = CASE WHEN source.capdata < ISNULL(target.capdata, '9999-12-31')
                                          THEN source.capdata ELSE target.capdata END
                , target.UpdateAt  = GETDATE()

        -- ── INSERT correos nuevos
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (correo, capdata, producto, CreateAt)
            VALUES (source.correo, source.capdata
                  , source.producto, GETDATE())

        OUTPUT $action INTO @MergeStats ([Action]);

        -- ── Métricas
        SET @registros_insertados   = (SELECT COUNT(*) FROM @MergeStats WHERE [Action] = 'INSERT')
        SET @registros_actualizados = (SELECT COUNT(*) FROM @MergeStats WHERE [Action] = 'UPDATE')
        SET @registros_procesados   = @registros_insertados + @registros_actualizados
        SET @descripcion            = 'Insertados: ' + CAST(@registros_insertados AS VARCHAR)
                                    + ' | Actualizados: ' + CAST(@registros_actualizados AS VARCHAR)
        SET @tiempo_segundos        = DATEDIFF(SECOND, @inicio, GETDATE())

        INSERT INTO [dw_etl].[dw_etl_audit_log]
               (nombre_procedimiento, registros_insertados, registros_actualizados
              , registros_procesados, estado, descripcion, tiempo_segundos)
        VALUES ('sp_upsert_factClientes', @registros_insertados, @registros_actualizados
              , @registros_procesados, @estado, @descripcion, @tiempo_segundos)

        PRINT @descripcion

    END TRY
    BEGIN CATCH
        SET @estado          = 'Error'
        SET @descripcion     = 'Error: ' + ERROR_MESSAGE()
        SET @tiempo_segundos = DATEDIFF(SECOND, @inicio, GETDATE())

        INSERT INTO [dw_etl].[dw_etl_audit_log]
               (nombre_procedimiento, registros_insertados, registros_actualizados
              , registros_procesados, estado, descripcion, tiempo_segundos)
        VALUES ('sp_upsert_factClientes', 0, 0, 0
              , @estado, @descripcion, @tiempo_segundos)

        PRINT @descripcion
    END CATCH
END
