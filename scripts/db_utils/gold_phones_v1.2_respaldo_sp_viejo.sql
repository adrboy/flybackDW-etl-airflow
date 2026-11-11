-- ============================================================
-- Gold teléfonos v1.2 — PASO 2a: RESPALDO DEL SP VIEJO
-- Base    : DBGeneralDW (SQL Server 244)
-- Fecha   : 2026-11-11
-- Qué hace: crea dw_etl.sp_insert_phones_factPersonalInfo_bk_20261111 con
--           el MISMO código que hoy tiene el SP desplegado. El original
--           NO se toca. La copia es solo respaldo: no se ejecuta.
-- Por qué una copia y no sp_rename: CREATE OR ALTER conserva los permisos
--           del SP original (por ejemplo el EXECUTE del usuario de Airflow);
--           renombrar y crear uno nuevo los perderia.
-- Ajustar la fecha del nombre al día en que se ejecute.
-- ============================================================
USE [DBGeneralDW];
GO

-- 1) Estado actual (solo lectura): fecha de modificación y versión probable
SELECT s.name              AS esquema
     , o.name              AS objeto
     , o.modify_date
     , CASE WHEN m.definition LIKE N'%THROW%'
            THEN 'tiene THROW (v1.1 o posterior)'
            ELSE 'sin THROW (v1.0)' END AS version_probable
     , LEN(m.definition)   AS caracteres
FROM sys.objects o
INNER JOIN sys.schemas s     ON s.schema_id = o.schema_id
INNER JOIN sys.sql_modules m ON m.object_id = o.object_id
WHERE o.name LIKE N'sp_insert_phones_factPersonalInfo%';
GO

-- 2) Crear la copia
DECLARE @origen sysname = N'sp_insert_phones_factPersonalInfo';
DECLARE @copia  sysname = N'sp_insert_phones_factPersonalInfo_bk_20261111';
DECLARE @def    NVARCHAR(MAX) = OBJECT_DEFINITION(OBJECT_ID(N'dw_etl.sp_insert_phones_factPersonalInfo'));
DECLARE @pos    INT;

IF @def IS NULL
    THROW 50010, N'No se pudo leer la definicion (el SP no existe o falta el permiso VIEW DEFINITION).', 1;

SET @pos = CHARINDEX(@origen, @def);

-- Resguardo: el primer nombre debe estar en el encabezado (CREATE ... PROCEDURE ...)
IF @pos = 0 OR @pos > 120 OR CHARINDEX(N'CREATE', @def) NOT BETWEEN 1 AND 20
    THROW 50011, N'La definicion no tiene el formato esperado: respaldar con SSMS (Script Stored Procedure as > CREATE To > File).', 1;

IF OBJECT_ID(N'dw_etl.' + @copia) IS NOT NULL
    THROW 50012, N'Ya existe una copia con ese nombre: usar otra fecha en @copia.', 1;

SET @def = STUFF(@def, @pos, LEN(@origen), @copia);
EXEC sp_executesql @def;
GO

-- 3) Verificar: deben existir las dos; el original conserva su modify_date
SELECT s.name              AS esquema
     , o.name              AS objeto
     , o.create_date
     , o.modify_date
     , LEN(m.definition)   AS caracteres
FROM sys.objects o
INNER JOIN sys.schemas s     ON s.schema_id = o.schema_id
INNER JOIN sys.sql_modules m ON m.object_id = o.object_id
WHERE o.name LIKE N'sp_insert_phones_factPersonalInfo%'
ORDER BY o.name;
GO

-- ------------------------------------------------------------
-- REVERSA: volver al SP anterior conservando permisos
-- (ejecutar solo si hace falta, después de haber creado la v1.2)
-- ------------------------------------------------------------
-- DECLARE @d NVARCHAR(MAX) = OBJECT_DEFINITION(OBJECT_ID(N'dw_etl.sp_insert_phones_factPersonalInfo_bk_20261111'));
-- SET @d = STUFF(@d, CHARINDEX(N'sp_insert_phones_factPersonalInfo_bk_20261111', @d)
--               , LEN(N'sp_insert_phones_factPersonalInfo_bk_20261111')
--               , N'sp_insert_phones_factPersonalInfo');
-- IF CHARINDEX(N'CREATE OR ALTER', @d) NOT BETWEEN 1 AND 20
--     SET @d = STUFF(@d, CHARINDEX(N'CREATE', @d), 6, N'ALTER');
-- EXEC sp_executesql @d;
--
-- Otra forma: dags/sql/clients/sp/gold/sp_insert_phones_factPersonalInfo.sql
-- (v1.1 en el repositorio; desplegarlo con CREATE OR ALTER).
