-- ============================================================
-- SP: dw_etl.sp_upsert_clientsfb_factClientesDetalle   (archivo de despliegue)
-- Base      : DBGeneralDW (SQL Server 244)
-- Ejecucion : manual en SSMS (CREATE OR ALTER)
-- Lo llama  : dw_etl.sp_etl_maestro (paso 2 de 6)
-- Origen    : source.clientsfb + source.clientescorreo + gral.factClientes
-- Destino   : gral.factClientesDetalle (un registro por certificado)
-- Version   : 1.1 - 2026-10-21
--             1.0: la que estaba desplegada. El CATCH anotaba 'Error' en
--                  dw_etl_audit_log y no relanzaba, asi que el maestro
--                  nunca se enteraba de un fallo del detalle.
--             1.1: el CATCH relanza el error (THROW) despues de anotarlo.
-- ============================================================

USE [DBGeneralDW]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dw_etl].[sp_upsert_clientsfb_factClientesDetalle]
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @inicio                 DATETIME        = GETDATE()
    DECLARE @registros_insertados   INT             = 0
    DECLARE @registros_actualizados INT             = 0
    DECLARE @registros_procesados   INT             = 0
    DECLARE @estado                 NVARCHAR(50)    = 'Success'
    DECLARE @descripcion            NVARCHAR(500)   = ''
    DECLARE @tiempo_segundos        DECIMAL(10,2)

    DECLARE @MergeStats TABLE ([Action] NVARCHAR(10))

    BEGIN TRY
        MERGE INTO [DBGeneralDW].[gral].[factClientesDetalle] AS target
        USING (
            SELECT
                  productid
                , idcorreo
                , contractid
                , clientid
                , bdate
                , capdata
                , FirstName
                , LastName
                , countrycode
                , country
                , Estate
                , ciudad
                , zip
                , corpcode
                , corp
                , ingreso
                , egreso
                , rank
                , EstatusN
                , EstatusL
                , deletedAt
            FROM (
                SELECT
                      a.productid
                    , fc.idcorreo
                    , a.contractid
                    , (a.clientid * 10) + 1                     AS clientid
                    , CAST('1900-01-01' AS DATE)                 AS bdate
                    , CAST(a.capdata AS DATE)                    AS capdata
                    , a.FirstName
                    , a.LastName
                    , a.countrycode
                    , a.country
                    , a.Estate
                    , a.ciudad
                    , a.zip
                    , TRY_CAST(a.corpcode AS INT)               AS corpcode
                    , a.corp
                    , a.ingreso
                    , a.egreso
                    , a.rank
                    , a.EstatusN
                    , a.EstatusL
                    , a.deletedAt
                    , ROW_NUMBER() OVER (PARTITION BY a.clientid ORDER BY a.createdAt DESC)                                         AS rn
                FROM [DBGeneralDW].[source].[clientsfb]            a
                INNER JOIN [DBGeneralDW].[source].[clientescorreo] ec ON  ec.email = LOWER(a.email) AND ec.productid = a.productid AND ec.clientid  = a.clientid
                INNER JOIN [DBGeneralDW].[gral].[factClientes]     fc ON  fc.correo = ec.email
                WHERE  a.email IS NOT NULL
                AND  TRIM(a.email) <> ''
                AND  a.email NOT LIKE '%@noreply.%'
            ) dedup
            WHERE rn = 1
        ) AS source
        ON target.clientid = source.clientid

        WHEN MATCHED AND (
            ISNULL(target.productid,            0)            <> ISNULL(source.productid,       0)
            OR ISNULL(target.idcorreo,          0)            <> ISNULL(source.idcorreo,        0)
            OR ISNULL(target.contractid,        '')           <> ISNULL(source.contractid,      '')
            OR ISNULL(target.capdata,           '1900-01-01') <> ISNULL(source.capdata,         '1900-01-01')
            OR ISNULL(target.NameCompleted,     '')           <> ISNULL(source.FirstName,       '')
            OR ISNULL(target.LastNameCompleted, '')           <> ISNULL(source.LastName,        '')
            OR ISNULL(target.countrycode,       '')           <> ISNULL(source.countrycode,     '')
            OR ISNULL(target.country,           '')           <> ISNULL(source.country,         '')
            OR ISNULL(target.state,             '')           <> ISNULL(source.Estate,          '')
            OR ISNULL(target.city,              '')           <> ISNULL(source.ciudad,          '')
            OR ISNULL(target.zip,               '')           <> ISNULL(source.zip,             '')
            OR ISNULL(target.corpcode,          0)            <> ISNULL(source.corpcode,        0)
            OR ISNULL(target.corp,              '')           <> ISNULL(source.corp,            '')
            OR ISNULL(target.ingreso,           0)            <> ISNULL(source.ingreso,         0)
            OR ISNULL(target.egreso,            0)            <> ISNULL(source.egreso,          0)
            OR ISNULL(target.rank,              0)            <> ISNULL(source.rank,            0)
            OR ISNULL(target.EstatusN,          0)            <> ISNULL(source.EstatusN,        0)
            OR ISNULL(target.EstatusL,          '')           <> ISNULL(source.EstatusL,        '')
            OR ISNULL(target.deletedAt,         '1900-01-01') <> ISNULL(source.deletedAt,       '1900-01-01')
        ) THEN
            UPDATE SET
                  target.productid            = source.productid
                , target.idcorreo             = source.idcorreo
                , target.contractid           = source.contractid
                , target.bdate               = source.bdate
                , target.capdata             = source.capdata
                , target.NameCompleted        = source.FirstName
                , target.LastNameCompleted    = source.LastName
                , target.countrycode          = source.countrycode
                , target.country             = source.country
                , target.state               = source.Estate
                , target.city                = source.ciudad
                , target.zip                 = source.zip
                , target.corpcode            = source.corpcode
                , target.corp                = source.corp
                , target.ingreso             = source.ingreso
                , target.egreso              = source.egreso
                , target.rank                = source.rank
                , target.EstatusN            = source.EstatusN
                , target.EstatusL            = source.EstatusL
                , target.updatedAt           = GETDATE()       --  hoy 
                , target.deletedAt           = source.deletedAt --  origen 
                -- target.createdAt NO se toca ← conserva original 

        WHEN NOT MATCHED THEN
            INSERT (
                  productid, idcorreo, contractid, clientid
                , bdate, capdata
                , NameCompleted, LastNameCompleted
                , countrycode, country, state, city, zip
                , corpcode, corp
                , ingreso, egreso, rank
                , EstatusN, EstatusL
                , createdAt, updatedAt, deletedAt
            )
            VALUES (
                  source.productid, source.idcorreo, source.contractid, source.clientid
                , source.bdate, source.capdata
                , source.FirstName, source.LastName
                , source.countrycode, source.country, source.Estate, source.ciudad, source.zip
                , source.corpcode, source.corp
                , source.ingreso, source.egreso, source.rank
                , source.EstatusN, source.EstatusL
                , GETDATE()          -- createdAt  hoy 
                , NULL               -- updatedAt  NULL, no modificado aún 
                , source.deletedAt   -- del origen 
            )

        OUTPUT $action INTO @MergeStats ([Action]);

        SET @registros_insertados   = (SELECT COUNT(*) FROM @MergeStats WHERE [Action] = 'INSERT')
        SET @registros_actualizados = (SELECT COUNT(*) FROM @MergeStats WHERE [Action] = 'UPDATE')
        SET @registros_procesados   = @registros_insertados + @registros_actualizados
        SET @descripcion            = 'Insertados: ' + CAST(@registros_insertados AS VARCHAR)
                                    + ' | Actualizados: ' + CAST(@registros_actualizados AS VARCHAR)
        SET @tiempo_segundos        = DATEDIFF(SECOND, @inicio, GETDATE())

        INSERT INTO [dw_etl].[dw_etl_audit_log] (
              nombre_procedimiento, registros_insertados, registros_actualizados
            , registros_procesados, estado, descripcion, tiempo_segundos
        )
        VALUES (
              'sp_upsert_clientsfb_factClientesDetalle'
            , @registros_insertados, @registros_actualizados
            , @registros_procesados, @estado, @descripcion, @tiempo_segundos
        )

        PRINT @descripcion

    END TRY
    BEGIN CATCH
        SET @estado          = 'Error'
        SET @descripcion     = 'Error: ' + ERROR_MESSAGE()
        SET @tiempo_segundos = DATEDIFF(SECOND, @inicio, GETDATE())

        INSERT INTO [dw_etl].[dw_etl_audit_log] (
              nombre_procedimiento, registros_insertados, registros_actualizados
            , registros_procesados, estado, descripcion, tiempo_segundos
        )
        VALUES (
              'sp_upsert_clientsfb_factClientesDetalle', 0, 0, 0
            , @estado, @descripcion, @tiempo_segundos
        )

        PRINT @descripcion;
        THROW;   -- relanza: el maestro acumula el error y Airflow lo ve como fallo
    END CATCH
END
