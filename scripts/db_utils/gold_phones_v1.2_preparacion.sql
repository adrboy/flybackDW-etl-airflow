-- ============================================================
-- Gold teléfonos v1.2 — PREPARACIÓN: VACIAR Y RECARGAR desde Silver
-- Base    : DBGeneralDW (SQL Server 244)
-- Tabla   : gral.factPersonalInfo
-- Fecha   : 2026-11-11 (borrador)
-- Motivo  : la tabla es el resultado de 5 cargas con formatos distintos
--           (espacios iniciales, '+', ceros, prefijos sueltos) y tiene
--           duplicados. Silver ya esta limpio: se recarga desde ahi.
-- Diseño  : UNA fila por (idcorreo, phone, productid) para siempre.
--           Retirar = deleteAt; volver = deleteAt NULL (revivir).
-- Orden   : 0 comprobaciones -> 1 respaldo -> 2 crear el SP
--           -> 3 vaciar + restriccion unica -> 4 recargar -> 5 verificar
--           -> 6 (opcional) createAt -> 7 idempotencia
-- Ejecutar en horario tranquilo: entre el paso 3 y el 4 la tabla esta vacia.
-- Correr UN paso a la vez.
-- ============================================================

-- ------------------------------------------------------------
-- PASO 0a — ¿quién lee factPersonalInfo? (solo lectura)
-- Revisar si alguno usa las columnas id o createAt.
-- ------------------------------------------------------------
SELECT OBJECT_SCHEMA_NAME(d.referencing_id) AS esquema
     , OBJECT_NAME(d.referencing_id)        AS objeto
     , o.type_desc
FROM sys.sql_expression_dependencies d
INNER JOIN sys.objects o ON o.object_id = d.referencing_id
WHERE d.referenced_schema_name = 'gral'
  AND d.referenced_entity_name = 'factPersonalInfo'
ORDER BY o.type_desc
       , objeto;

-- PASO 0b — llaves foráneas hacia la tabla: debe dar 0 filas
SELECT OBJECT_NAME(parent_object_id) AS tabla_que_referencia
     , name                          AS llave_foranea
FROM sys.foreign_keys
WHERE referenced_object_id = OBJECT_ID('gral.factPersonalInfo');

-- ------------------------------------------------------------
-- PASO 0c — ¿está al día factClientesDetalle? (de ahí sale el idcorreo)
-- en_detalle debería ser >= silver_vivos. Si es MUY menor, correr primero
-- el Gold de clientes: la recarga de teléfonos solo cubre clientes mapeados.
-- ------------------------------------------------------------
SELECT 1 AS productid
     , (SELECT COUNT(*) FROM source.clientsfb WHERE deletedAt IS NULL) AS silver_vivos
     , (SELECT COUNT(*) FROM gral.factClientesDetalle WHERE clientid % 10 = 1) AS en_detalle
UNION ALL
SELECT 2
     , (SELECT COUNT(*) FROM source.clientsbb WHERE deletedAt IS NULL)
     , (SELECT COUNT(*) FROM gral.factClientesDetalle WHERE clientid % 10 = 2)
UNION ALL
SELECT 3
     , (SELECT COUNT(*) FROM source.clientsfi WHERE deletedAt IS NULL)
     , (SELECT COUNT(*) FROM gral.factClientesDetalle WHERE clientid % 10 = 3)
UNION ALL
SELECT 4
     , (SELECT COUNT(*) FROM source.clientsvc WHERE deletedAt IS NULL)
     , (SELECT COUNT(*) FROM gral.factClientesDetalle WHERE clientid % 10 = 4)
UNION ALL
SELECT 5
     , (SELECT COUNT(*) FROM source.clientsml WHERE deletedAt IS NULL)
     , (SELECT COUNT(*) FROM gral.factClientesDetalle WHERE clientid % 10 = 5);

-- ------------------------------------------------------------
-- PASO 0d — tamaño esperado de la recarga (teléfonos vivos de Silver
-- ya traducidos a idcorreo, sin repetidos). Apuntar los 5 números.
-- ------------------------------------------------------------
SELECT 1 AS productid, COUNT(*) AS filas_a_cargar
FROM (SELECT DISTINCT fc.idcorreo, ph.phone
      FROM source.Phonefb ph
      INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 1
      INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
      WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> '') x
UNION ALL
SELECT 2, COUNT(*)
FROM (SELECT DISTINCT fc.idcorreo, ph.phone
      FROM source.Phonebb ph
      INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 2
      INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
      WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> '') x
UNION ALL
SELECT 3, COUNT(*)
FROM (SELECT DISTINCT fc.idcorreo, ph.phone
      FROM source.Phonefi ph
      INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 3
      INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
      WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> '') x
UNION ALL
SELECT 4, COUNT(*)
FROM (SELECT DISTINCT fc.idcorreo, ph.phone
      FROM source.Phonevc ph
      INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 4
      INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
      WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> '') x
UNION ALL
SELECT 5, COUNT(*)
FROM (SELECT DISTINCT fc.idcorreo, ph.phone
      FROM source.Phoneml ph
      INNER JOIN gral.factClientesDetalle fcd ON fcd.clientid = ph.clientid * 10 + 5
      INNER JOIN gral.factClientes fc ON fc.idcorreo = fcd.idcorreo
      WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> '') x;


-- ------------------------------------------------------------
-- PASO 1 — RESPALDO COMPLETO (obligatorio)
-- Ajustar la fecha del nombre al día en que se ejecute.
-- ------------------------------------------------------------
SELECT *
INTO gral.factPersonalInfo_bk_20261111
FROM gral.factPersonalInfo;

SELECT (SELECT COUNT(*) FROM gral.factPersonalInfo)             AS original
     , (SELECT COUNT(*) FROM gral.factPersonalInfo_bk_20261111) AS respaldo;
-- Deben ser iguales. Si no, NO seguir.


-- ------------------------------------------------------------
-- PASO 2 — crear el SP (gold_phones_v1.2_sp_borrador.sql)
-- Se crea ANTES de vaciar, para que la tabla este vacia el menor tiempo.
-- ------------------------------------------------------------


-- ------------------------------------------------------------
-- PASO 3 — VACIAR y poner la restricción única
-- Se usa DELETE y no TRUNCATE: asi el contador de id continúa donde iba
-- (TRUNCATE lo reiniciaria en 1).
-- Ejecutar el paso 3 y el 4 seguidos.
-- ------------------------------------------------------------
DELETE FROM gral.factPersonalInfo;

ALTER TABLE gral.factPersonalInfo
    ADD CONSTRAINT UQ_factPersonalInfo_correo_phone_producto
    UNIQUE (idcorreo, phone, productid);


-- ------------------------------------------------------------
-- PASO 4 — RECARGAR desde Silver, de a un producto
-- Tabla vacia: la compuerta no interviene (vivos = 0).
-- Cada llamada devuelve insertados / revividos / retirados.
-- Los insertados deben coincidir con filas_a_cargar del paso 0d.
-- (O sin parametros para los 5 de una vez.)
-- ------------------------------------------------------------
-- EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 5;  -- ml
-- EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 3;  -- fi
-- EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 4;  -- vc
-- EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 2;  -- bb
-- EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 1;  -- fb


-- ------------------------------------------------------------
-- PASO 5 — VERIFICAR: antes (respaldo) contra después
-- vivas_despues debe coincidir con filas_a_cargar (0d). Una caída fuerte
-- frente a vivas_antes obliga a revisar factClientesDetalle (paso 0c).
-- ------------------------------------------------------------
SELECT n.productid
     , (SELECT COUNT(*) FROM gral.factPersonalInfo_bk_20261111 b
        WHERE b.productid = n.productid AND b.deleteAt IS NULL) AS vivas_antes
     , (SELECT COUNT(*) FROM gral.factPersonalInfo g
        WHERE g.productid = n.productid AND g.deleteAt IS NULL) AS vivas_despues
FROM (VALUES (1), (2), (3), (4), (5)) AS n (productid)
ORDER BY n.productid;

-- Duplicados: debe devolver 0 filas
SELECT idcorreo, phone, productid, COUNT(*) AS veces
FROM gral.factPersonalInfo
GROUP BY idcorreo, phone, productid
HAVING COUNT(*) > 1;


-- ------------------------------------------------------------
-- PASO 6 — (OPCIONAL) conservar el createAt original de los teléfonos que
-- sobreviven con exactamente el mismo texto. Solo si algun reporte usa
-- createAt (ver paso 0a). Los teléfonos nuevos conservan la fecha de hoy.
-- ------------------------------------------------------------
-- UPDATE g
-- SET g.createAt = b.primera
-- FROM gral.factPersonalInfo g
-- INNER JOIN (SELECT idcorreo, phone, productid, MIN(createAt) AS primera
--             FROM gral.factPersonalInfo_bk_20261111
--             GROUP BY idcorreo, phone, productid) b
--         ON b.idcorreo  = g.idcorreo
--        AND b.phone     = g.phone
--        AND b.productid = g.productid
-- WHERE b.primera < g.createAt;


-- ------------------------------------------------------------
-- PASO 7 — IDEMPOTENCIA: correr el SP sin parámetros. Debe dar 0 / 0 / 0.
-- ------------------------------------------------------------
-- EXEC dw_etl.sp_insert_phones_factPersonalInfo;

-- Historial de ejecuciones del SP:
SELECT TOP 20 *
FROM dw_etl.dw_etl_audit_log
WHERE nombre_procedimiento = 'sp_insert_phones_factPersonalInfo'
ORDER BY 1 DESC;


-- ------------------------------------------------------------
-- LIMPIEZA ANUAL (cuando se quiera): borrar lo retirado hace más de un año
-- ------------------------------------------------------------
-- DELETE FROM gral.factPersonalInfo
-- WHERE deleteAt < DATEADD(YEAR, -1, SYSDATETIME());


-- ------------------------------------------------------------
-- REVERSA (volver al estado del respaldo)
-- ------------------------------------------------------------
-- ALTER TABLE gral.factPersonalInfo DROP CONSTRAINT UQ_factPersonalInfo_correo_phone_producto;
-- DELETE FROM gral.factPersonalInfo;
-- SET IDENTITY_INSERT gral.factPersonalInfo ON;
-- INSERT INTO gral.factPersonalInfo (id, idcorreo, phone, productid, createAt, updateAt, deleteAt)
-- SELECT id, idcorreo, phone, productid, createAt, updateAt, deleteAt
-- FROM gral.factPersonalInfo_bk_20261111;
-- SET IDENTITY_INSERT gral.factPersonalInfo OFF;
-- Volver al SP anterior: desplegar de nuevo
--   dags/sql/clients/sp/gold/sp_insert_phones_factPersonalInfo.sql (v1.1)
