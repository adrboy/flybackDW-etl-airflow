-- ============================================================
-- SP: dw_etl.sp_etl_maestro   (archivo de despliegue)
-- Base      : DBGeneralDW (SQL Server 244)
-- Ejecucion : manual en SSMS (CREATE OR ALTER)
-- Lo llama  : dag_master_gold -> sql/clients/exec_sp_maestro.sql
-- Version   : 1.1 - 2026-10-21
--             1.0: la que estaba desplegada. Los errores se tragaban:
--                  el CATCH solo hacia PRINT y el RETURN tras fallar la
--                  base terminaba sin error, asi que Airflow siempre
--                  veia exito.
--             1.1: los errores llegan a Airflow.
--                  - El CATCH externo relanza el error (THROW).
--                  - Si falla la base, o no deja fila de ESTA corrida,
--                    THROW en lugar de RETURN.
--                  - Los detalles siguen siendo independientes, pero se
--                    acumula el mensaje de cada error y, al final, si hubo
--                    alguno, THROW con el resumen.
--                  - La verificacion de la base solo mira filas de esta
--                    corrida (fecha_ejecucion >= inicio - 1 s).
-- ============================================================

USE [DBGeneralDW]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dw_etl].[sp_etl_maestro]
AS
BEGIN
	SET NOCOUNT ON;
	
	DECLARE @inicio DATETIME = GETDATE()
	DECLARE @estado_base NVARCHAR(50)
	DECLARE @tiempo_total DECIMAL(10,2)
	DECLARE @errores NVARCHAR(MAX) = N''
	DECLARE @msg NVARCHAR(2048)
	
	PRINT '========================================='
	PRINT 'INICIANDO ETL MAESTRO'
	PRINT 'Fecha: ' + CAST(GETDATE() AS NVARCHAR(30))
	PRINT '========================================='
	
	BEGIN TRY
		-- PASO 1: EJECUTAR SP BASE (factClientes)
		PRINT ''
		PRINT '[1/6] Ejecutando sp_upsert_factClientes (BASE)...'
		EXEC [dw_etl].[sp_upsert_factClientes]
		
		-- Verificar si la base falló
		SELECT TOP 1 @estado_base = estado 
		FROM [dw_etl].[dw_etl_audit_log] 
		WHERE nombre_procedimiento = 'sp_upsert_factClientes'
		  AND fecha_ejecucion >= DATEADD(SECOND, -1, @inicio)
		ORDER BY id DESC
		
		-- SI LA BASE FALLÓ, DETENER TODO
		IF @estado_base IS NULL OR @estado_base = 'Error'
		BEGIN
			PRINT ''
			PRINT '❌ ERROR CRÍTICO: Falló sp_upsert_factClientes'
			PRINT 'NO se ejecutarán los demás procedimientos'
			PRINT 'Por favor revise el error y reinicie el ETL';
			THROW 50002, 'Fallo sp_upsert_factClientes (o no dejo fila de esta corrida); no se ejecutaron los detalles', 1;
		END
		
		PRINT '✓ sp_upsert_factClientes completado exitosamente'
		
		-- PASO 2: EJECUTAR LOS 5 SP (con manejo independiente de errores)
		PRINT ''
		PRINT '[2/6] Ejecutando sp_upsert_clientsfb_factClientesDetalle...'
		BEGIN TRY
			EXEC [dw_etl].[sp_upsert_clientsfb_factClientesDetalle]
			PRINT '✓ sp_upsert_clientsfb_factClientesDetalle completado'
		END TRY
		BEGIN CATCH
			SET @errores += N'[fb] ' + ERROR_MESSAGE() + N' | ';
			PRINT '❌ Error en sp_upsert_clientsfb_factClientesDetalle (continuando con siguientes)'
		END CATCH
		
		PRINT ''
		PRINT '[3/6] Ejecutando sp_upsert_clientsbb_factClientesDetalle...'
		BEGIN TRY
			EXEC [dw_etl].[sp_upsert_clientsbb_factClientesDetalle]
			PRINT '✓ sp_upsert_clientsbb_factClientesDetalle completado'
		END TRY
		BEGIN CATCH
			SET @errores += N'[bb] ' + ERROR_MESSAGE() + N' | ';
			PRINT '❌ Error en sp_upsert_clientsbb_factClientesDetalle (continuando con siguientes)'
		END CATCH
		
		PRINT ''
		PRINT '[4/6] Ejecutando sp_upsert_clientsfi_factClientesDetalle...'
		BEGIN TRY
			EXEC [dw_etl].[sp_upsert_clientsfi_factClientesDetalle]
			PRINT '✓ sp_upsert_clientsfi_factClientesDetalle completado'
		END TRY
		BEGIN CATCH
			SET @errores += N'[fi] ' + ERROR_MESSAGE() + N' | ';
			PRINT '❌ Error en sp_upsert_clientsfi_factClientesDetalle (continuando con siguientes)'
		END CATCH
		
		PRINT ''
		PRINT '[5/6] Ejecutando sp_upsert_clientsvc_factClientesDetalle...'
		BEGIN TRY
			EXEC [dw_etl].[sp_upsert_clientsvc_factClientesDetalle]
			PRINT '✓ sp_upsert_clientsvc_factClientesDetalle completado'
		END TRY
		BEGIN CATCH
			SET @errores += N'[vc] ' + ERROR_MESSAGE() + N' | ';
			PRINT '❌ Error en sp_upsert_clientsvc_factClientesDetalle (continuando con siguientes)'
		END CATCH
		
		PRINT ''
		PRINT '[6/6] Ejecutando sp_upsert_clientsml_factClientesDetalle...'
		BEGIN TRY
			EXEC [dw_etl].[sp_upsert_clientsml_factClientesDetalle]
			PRINT '✓ sp_upsert_clientsml_factClientesDetalle completado'
		END TRY
		BEGIN CATCH
			SET @errores += N'[ml] ' + ERROR_MESSAGE() + N' | ';
			PRINT '❌ Error en sp_upsert_clientsml_factClientesDetalle'
		END CATCH
		
		-- RESUMEN FINAL
		SET @tiempo_total = DATEDIFF(SECOND, @inicio, GETDATE())
		
		PRINT ''
		PRINT '========================================='
		PRINT 'ETL MAESTRO FINALIZADO'
		PRINT 'Tiempo total: ' + CAST(@tiempo_total AS NVARCHAR(10)) + ' segundos'
		PRINT '========================================='
		PRINT ''
		PRINT 'RESUMEN DE EJECUCIÓN:'
		SELECT 
			nombre_procedimiento,
			registros_insertados,
			registros_actualizados,
			registros_procesados,
			estado,
			fecha_ejecucion
		FROM [dw_etl].[dw_etl_audit_log]
		WHERE fecha_ejecucion >= DATEADD(MINUTE, -5, GETDATE())
		ORDER BY id DESC
		
		-- Si algun detalle fallo, relanzar para que Airflow lo vea como fallo
		IF @errores <> N''
		BEGIN
			SET @msg = LEFT(N'ETL maestro termino con errores en detalles: ' + @errores, 2000);
			THROW 50001, @msg, 1;
		END
		
	END TRY
	BEGIN CATCH
		PRINT '❌ ERROR FATAL EN ETL MAESTRO: ' + ERROR_MESSAGE();
		THROW;
	END CATCH
END
