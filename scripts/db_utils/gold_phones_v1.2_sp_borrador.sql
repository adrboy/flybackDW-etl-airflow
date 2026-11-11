-- ============================================================
-- SP: dw_etl.sp_insert_phones_factPersonalInfo   (BORRADOR v1.2 - MERGE)
-- Base      : DBGeneralDW (SQL Server 244)
-- Ejecucion : manual en SSMS (CREATE OR ALTER) -- NO desplegado todavia
-- Lo llama  : dag_phones_quincenal (tarea gold_phones, pendiente)
--             -> sql/clients/exec_sp_phones.sql (sin parametros = los 5)
-- Origen    : source.Phonefb, Phonebb, Phonefi, Phonevc, Phoneml
--             (solo filas vivas: deletedAt IS NULL)
--             + gral.factClientesDetalle + gral.factClientes
-- Destino   : gral.factPersonalInfo (idcorreo + phone + productid)
--
-- Version   : 1.2 - 2026-11-10 (borrador)
--             1.0: solo insertaba; el CATCH no relanzaba.
--             1.1: el CATCH relanza el error (THROW).
--             1.2: MERGE sobre la llave unica (idcorreo, phone, productid)
--                  (requiere UQ_factPersonalInfo_correo_phone_producto):
--                  - NOT MATCHED BY TARGET  -> INSERT (telefono nuevo)
--                  - MATCHED y deleteAt NOT NULL -> REVIVIR (deleteAt = NULL)
--                  - NOT MATCHED BY SOURCE y vivo -> RETIRAR (deleteAt = ahora)
--                  - MATCHED y vivo -> nada (idempotente: 2da corrida = 0/0/0)
--                  No existe UPDATE de un telefono "modificado": el numero es
--                  parte de la llave; uno corregido = uno retirado + uno nuevo.
--                  Una sola fila por telefono (no hay fila nueva al volver).
--                  OUTPUT del MERGE -> cuenta insertados / revividos / retirados.
--                  Parametros: @productid (NULL = los 5), @max_pct, @forzar,
--                  @simular (calcula y NO escribe).
--                  Compuerta: si el retiro pasa de @max_pct de los vivos del
--                  alcance, se detiene SIN escribir nada.
--                  Una sola transaccion y una sola ejecucion (sp_getapplock).
--
-- Ejemplos:
--   EXEC dw_etl.sp_insert_phones_factPersonalInfo;
--        -> los 5 productos, limite 2% (lo que usa el DAG)
--   EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 5, @simular = 1;
--        -> solo ml: cuantos insertaria / revivira / retiraria, sin escribir
--   EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 5, @forzar = 1;
--        -> solo ml, ignorando la compuerta (primera ejecucion)
--
-- Simulacro de retiros del 2026-11-10 (antes de v1.2):
--   ml 9 | fi 574 | vc 1,011 | bb 236 (2.2%) | fb 25,946 (9.1%)
--
-- Requisito: gral.factClientesDetalle al dia (el Gold de clientes primero),
--            porque el idcorreo de cada telefono sale de ahi.
-- ============================================================

USE [DBGeneralDW]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE OR ALTER PROCEDURE [dw_etl].[sp_insert_phones_factPersonalInfo]
      @productid INT          = NULL    -- NULL = los 5 productos; 1..5 = uno solo
    , @max_pct   DECIMAL(8,4) = 2       -- % maximo de filas vivas que puede retirar
    , @forzar    BIT          = 0       -- 1 = ignorar la compuerta
    , @simular   BIT          = 0       -- 1 = calcular y NO escribir
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @inicio          DATETIME       = GETDATE()
          , @ahora           DATETIME2(7)
          , @a_insertar      INT            = 0
          , @a_revivir       INT            = 0
          , @por_retirar     INT            = 0
          , @vivos           INT            = 0
          , @insertados      INT            = 0
          , @revividos       INT            = 0
          , @retirados       INT            = 0
          , @vivos_final     INT            = 0
          , @lock            INT
          , @estado          NVARCHAR(50)   = N'Success'
          , @descripcion     NVARCHAR(500)  = N''
          , @tiempo_segundos DECIMAL(10,2)
          , @alcance         NVARCHAR(20)   = CASE WHEN @productid IS NULL
                                                   THEN N'5 productos'
                                                   ELSE N'producto ' + CAST(@productid AS NVARCHAR(10))
                                              END

    IF @productid IS NOT NULL AND @productid NOT BETWEEN 1 AND 5
        THROW 50003, N'@productid debe ser NULL o un valor entre 1 y 5', 1;

    BEGIN TRY
        BEGIN TRAN;

        -- Una sola ejecucion a la vez (el candado se libera solo al terminar la transaccion)
        EXEC @lock = sp_getapplock @Resource    = N'sp_insert_phones_factPersonalInfo'
                                 , @LockMode    = N'Exclusive'
                                 , @LockOwner   = N'Transaction'
                                 , @LockTimeout = 300000;
        IF @lock < 0
            THROW 50002, N'Ya hay una ejecucion de sp_insert_phones_factPersonalInfo en curso', 1;

        -- --------------------------------------------------------
        -- 1) Telefonos VIVOS en Silver, ya traducidos a idcorreo
        --    (phone con la collation de la base: las temporales nacen
        --     en tempdb con otra y el JOIN fallaria)
        -- --------------------------------------------------------
        CREATE TABLE #vivos (
              productid INT          NOT NULL
            , idcorreo  INT          NOT NULL
            , phone     NVARCHAR(33) COLLATE DATABASE_DEFAULT NOT NULL
            , PRIMARY KEY (productid, idcorreo, phone)
        );

        IF @productid IS NULL OR @productid = 1
            INSERT INTO #vivos (productid, idcorreo, phone)
            SELECT DISTINCT 1, fc.idcorreo, ph.phone
            FROM source.Phonefb ph
            INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 1
            INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
            WHERE ph.deletedAt IS NULL
              AND ph.phone IS NOT NULL
              AND LTRIM(RTRIM(ph.phone)) <> '';

        IF @productid IS NULL OR @productid = 2
            INSERT INTO #vivos (productid, idcorreo, phone)
            SELECT DISTINCT 2, fc.idcorreo, ph.phone
            FROM source.Phonebb ph
            INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 2
            INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
            WHERE ph.deletedAt IS NULL
              AND ph.phone IS NOT NULL
              AND LTRIM(RTRIM(ph.phone)) <> '';

        IF @productid IS NULL OR @productid = 3
            INSERT INTO #vivos (productid, idcorreo, phone)
            SELECT DISTINCT 3, fc.idcorreo, ph.phone
            FROM source.Phonefi ph
            INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 3
            INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
            WHERE ph.deletedAt IS NULL
              AND ph.phone IS NOT NULL
              AND LTRIM(RTRIM(ph.phone)) <> '';

        IF @productid IS NULL OR @productid = 4
            INSERT INTO #vivos (productid, idcorreo, phone)
            SELECT DISTINCT 4, fc.idcorreo, ph.phone
            FROM source.Phonevc ph
            INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 4
            INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
            WHERE ph.deletedAt IS NULL
              AND ph.phone IS NOT NULL
              AND LTRIM(RTRIM(ph.phone)) <> '';

        IF @productid IS NULL OR @productid = 5
            INSERT INTO #vivos (productid, idcorreo, phone)
            SELECT DISTINCT 5, fc.idcorreo, ph.phone
            FROM source.Phoneml ph
            INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 5
            INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
            WHERE ph.deletedAt IS NULL
              AND ph.phone IS NOT NULL
              AND LTRIM(RTRIM(ph.phone)) <> '';

        -- --------------------------------------------------------
        -- 2) Cuanto haria el MERGE (ANTES de escribir nada)
        -- --------------------------------------------------------
        SELECT @vivos = COUNT(*)
        FROM gral.factPersonalInfo
        WHERE deleteAt IS NULL
          AND productid BETWEEN 1 AND 5
          AND (@productid IS NULL OR productid = @productid);

        SELECT @a_insertar = COUNT(*)
        FROM #vivos v
        WHERE NOT EXISTS (SELECT 1 FROM gral.factPersonalInfo t
                          WHERE t.productid = v.productid
                            AND t.idcorreo  = v.idcorreo
                            AND t.phone     = v.phone);

        SELECT @a_revivir = COUNT(*)
        FROM #vivos v
        WHERE EXISTS (SELECT 1 FROM gral.factPersonalInfo t
                      WHERE t.productid = v.productid
                        AND t.idcorreo  = v.idcorreo
                        AND t.phone     = v.phone
                        AND t.deleteAt IS NOT NULL);

        SELECT @por_retirar = COUNT(*)
        FROM gral.factPersonalInfo g
        WHERE g.deleteAt IS NULL
          AND g.productid BETWEEN 1 AND 5
          AND (@productid IS NULL OR g.productid = @productid)
          AND NOT EXISTS (SELECT 1 FROM #vivos v
                          WHERE v.productid = g.productid
                            AND v.idcorreo  = g.idcorreo
                            AND v.phone     = g.phone);

        -- Simulacro: informa y NO escribe
        IF @simular = 1
        BEGIN
            ROLLBACK;
            SELECT N'SIMULACRO' AS modo
                 , @alcance     AS alcance
                 , @vivos       AS vivos_en_gold
                 , @a_insertar  AS se_insertarian
                 , @a_revivir   AS se_revivirian
                 , @por_retirar AS se_retirarian;
            RETURN;
        END

        -- Compuerta: se detiene sin haber escrito nada
        IF @forzar = 0 AND @vivos > 0 AND @por_retirar * 100.0 > @vivos * @max_pct
        BEGIN
            SET @descripcion = N'BLOQUEADO: retiraria ' + CAST(@por_retirar AS NVARCHAR(20))
                             + N' de ' + CAST(@vivos AS NVARCHAR(20))
                             + N' vivos (limite ' + CAST(@max_pct AS NVARCHAR(20))
                             + N'%). Usar @forzar = 1';
            THROW 50001, @descripcion, 1;
        END

        SET @ahora = SYSDATETIME();

        -- --------------------------------------------------------
        -- 3) MERGE sobre la llave unica (idcorreo, phone, productid)
        -- --------------------------------------------------------
        CREATE TABLE #accion (
              accion  NVARCHAR(10) NOT NULL
            , antes   DATETIME2(7) NULL     -- deleteAt antes de la accion
            , despues DATETIME2(7) NULL     -- deleteAt despues de la accion
        );

        MERGE gral.factPersonalInfo AS t
        USING #vivos AS s
           ON  t.productid = s.productid
           AND t.idcorreo  = s.idcorreo
           AND t.phone     = s.phone
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (idcorreo, phone, productid, createAt)
            VALUES (s.idcorreo, s.phone, s.productid, @ahora)
        WHEN MATCHED AND t.deleteAt IS NOT NULL THEN
            UPDATE SET t.deleteAt = NULL
                     , t.updateAt = @ahora
        WHEN NOT MATCHED BY SOURCE
             AND t.deleteAt IS NULL
             AND t.productid BETWEEN 1 AND 5
             AND (@productid IS NULL OR t.productid = @productid) THEN
            UPDATE SET t.deleteAt = @ahora
                     , t.updateAt = @ahora
        OUTPUT $action, deleted.deleteAt, inserted.deleteAt
          INTO #accion (accion, antes, despues);

        SELECT @insertados = ISNULL(SUM(CASE WHEN accion = 'INSERT' THEN 1 ELSE 0 END), 0)
             , @revividos  = ISNULL(SUM(CASE WHEN accion = 'UPDATE' AND despues IS NULL     THEN 1 ELSE 0 END), 0)
             , @retirados  = ISNULL(SUM(CASE WHEN accion = 'UPDATE' AND despues IS NOT NULL THEN 1 ELSE 0 END), 0)
        FROM #accion;

        SELECT @vivos_final = COUNT(*)
        FROM gral.factPersonalInfo
        WHERE deleteAt IS NULL
          AND productid BETWEEN 1 AND 5
          AND (@productid IS NULL OR productid = @productid);

        COMMIT;

        SET @tiempo_segundos = DATEDIFF(SECOND, @inicio, GETDATE())
        SET @descripcion = N'[' + @alcance + N'] insertados ' + CAST(@insertados AS NVARCHAR(20))
                         + N', revividos ' + CAST(@revividos AS NVARCHAR(20))
                         + N', retirados ' + CAST(@retirados AS NVARCHAR(20))
                         + N', vivos finales ' + CAST(@vivos_final AS NVARCHAR(20))

        INSERT INTO [dw_etl].[dw_etl_audit_log] (nombre_procedimiento, registros_insertados, registros_actualizados, registros_procesados, estado, descripcion, tiempo_segundos)
        VALUES ('sp_insert_phones_factPersonalInfo', @insertados, @revividos + @retirados, @insertados + @revividos + @retirados, @estado, @descripcion, @tiempo_segundos)

        PRINT @descripcion

        SELECT N'EJECUTADO'    AS modo
             , @alcance        AS alcance
             , @insertados     AS insertados
             , @revividos      AS revividos
             , @retirados      AS retirados
             , @vivos_final    AS vivos_final
             , @tiempo_segundos AS segundos;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;

        DECLARE @num INT            = ERROR_NUMBER()
              , @msg NVARCHAR(2000) = ERROR_MESSAGE();

        SET @estado = CASE WHEN @num = 50001 THEN N'Blocked' ELSE N'Error' END
        SET @descripcion = LEFT(N'[' + @alcance + N'] ' + @msg, 500)
        SET @tiempo_segundos = DATEDIFF(SECOND, @inicio, GETDATE())

        INSERT INTO [dw_etl].[dw_etl_audit_log] (nombre_procedimiento, registros_insertados, registros_actualizados, registros_procesados, estado, descripcion, tiempo_segundos)
        VALUES ('sp_insert_phones_factPersonalInfo', 0, 0, 0, @estado, @descripcion, @tiempo_segundos)

        PRINT @descripcion;
        THROW;   -- relanza: Airflow ve el fallo en la tarea gold_phones
    END CATCH
END;
GO
