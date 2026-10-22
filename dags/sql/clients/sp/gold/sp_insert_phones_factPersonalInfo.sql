-- ============================================================
-- SP: dw_etl.sp_insert_phones_factPersonalInfo   (archivo de despliegue)
-- Base      : DBGeneralDW (SQL Server 244)
-- Ejecucion : manual en SSMS (CREATE OR ALTER)
-- Lo llama  : dag_master_gold -> sql/clients/exec_sp_phones.sql (tarea 2 de 2)
-- Origen    : source.Phonefb, Phonebb, Phonefi, Phonevc, Phoneml
--             + gral.factClientesDetalle + gral.factClientes
-- Destino   : gral.factPersonalInfo (idcorreo + telefono + producto)
-- Version   : 1.1 - 2026-10-22
--             1.0: la que estaba desplegada (modificada por ultima vez el
--                  2025-10-28). Solo inserta; el CATCH anotaba 'Error' en
--                  dw_etl_audit_log y no relanzaba, asi que Airflow veia exito.
--             1.1: el CATCH relanza el error (THROW) despues de anotarlo.
-- Pendiente : normalizar los telefonos en la capa on-premises (vistas
--             vwpersonalinfo*) y definir que pasa con los telefonos que
--             salen del origen (hoy nunca se marca deleteAt).
-- ============================================================

USE [DBGeneralDW]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dw_etl].[sp_insert_phones_factPersonalInfo]
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @inicio DATETIME = GETDATE()
    DECLARE @registros_insertados INT = 0
    DECLARE @estado NVARCHAR(50) = 'Success'
    DECLARE @descripcion NVARCHAR(500) = ''
    DECLARE @tiempo_segundos DECIMAL(10,2)
    
    BEGIN TRY
        -- Insertar teléfonos ya normalizados desde las 5 tablas source
        INSERT INTO [DBGeneralDW].[gral].[factPersonalInfo] (idcorreo, phone, productid, createAt)
        SELECT DISTINCT
            fc.idcorreo
            , ph.phone
            , 1 AS productid
            , GETDATE()
        FROM source.Phonefb ph
        INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 1
        INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
        WHERE ph.phone IS NOT NULL 
          AND LTRIM(RTRIM(ph.phone)) <> ''
          AND NOT EXISTS (
              SELECT 1 FROM [DBGeneralDW].[gral].[factPersonalInfo] target
              WHERE target.idcorreo = fc.idcorreo
                AND target.phone = ph.phone
                AND target.productid = 1
                AND target.deleteAt IS NULL
          )
        
        UNION
        
        SELECT DISTINCT fc.idcorreo, ph.phone, 2, GETDATE()
        FROM source.Phonebb ph
        INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 2
        INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
        WHERE ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
          AND NOT EXISTS (
              SELECT 1 FROM [DBGeneralDW].[gral].[factPersonalInfo] target
              WHERE target.idcorreo = fc.idcorreo AND target.phone = ph.phone 
                AND target.productid = 2 AND target.deleteAt IS NULL
          )
        
        UNION
        
        SELECT DISTINCT fc.idcorreo, ph.phone, 3, GETDATE()
        FROM source.Phonefi ph
        INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 3
        INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
        WHERE ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
          AND NOT EXISTS (
              SELECT 1 FROM [DBGeneralDW].[gral].[factPersonalInfo] target
              WHERE target.idcorreo = fc.idcorreo AND target.phone = ph.phone 
                AND target.productid = 3 AND target.deleteAt IS NULL
          )
        
        UNION
        
        SELECT DISTINCT fc.idcorreo, ph.phone, 4, GETDATE()
        FROM source.Phonevc ph
        INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 4
        INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
        WHERE ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
          AND NOT EXISTS (
              SELECT 1 FROM [DBGeneralDW].[gral].[factPersonalInfo] target
              WHERE target.idcorreo = fc.idcorreo AND target.phone = ph.phone 
                AND target.productid = 4 AND target.deleteAt IS NULL
          )
        
        UNION
        
        SELECT DISTINCT fc.idcorreo, ph.phone, 5, GETDATE()
        FROM source.Phoneml ph
        INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 5
        INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
        WHERE ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
          AND NOT EXISTS (
              SELECT 1 FROM [DBGeneralDW].[gral].[factPersonalInfo] target
              WHERE target.idcorreo = fc.idcorreo AND target.phone = ph.phone 
                AND target.productid = 5 AND target.deleteAt IS NULL
          );
        
        SET @registros_insertados = @@ROWCOUNT
        SET @descripcion = 'Se insertaron ' + CAST(@registros_insertados AS VARCHAR) + ' teléfonos nuevos'
        SET @tiempo_segundos = DATEDIFF(SECOND, @inicio, GETDATE())
        
        INSERT INTO [dw_etl].[dw_etl_audit_log] (nombre_procedimiento, registros_insertados, registros_actualizados, registros_procesados, estado, descripcion, tiempo_segundos)
        VALUES ('sp_insert_phones_factPersonalInfo', @registros_insertados, 0, @registros_insertados, @estado, @descripcion, @tiempo_segundos)
        
        PRINT @descripcion
        
    END TRY
    BEGIN CATCH
        SET @estado = 'Error'
        SET @descripcion = 'Error en sp_insert_phones_factPersonalInfo: ' + ERROR_MESSAGE()
        SET @tiempo_segundos = DATEDIFF(SECOND, @inicio, GETDATE())
        
        INSERT INTO [dw_etl].[dw_etl_audit_log] (nombre_procedimiento, registros_insertados, registros_actualizados, registros_procesados, estado, descripcion, tiempo_segundos)
        VALUES ('sp_insert_phones_factPersonalInfo', 0, 0, 0, @estado, @descripcion, @tiempo_segundos)
        
        PRINT @descripcion;
        THROW;   -- relanza: Airflow ve el fallo en la tarea sp_insert_phones
    END CATCH
END;
