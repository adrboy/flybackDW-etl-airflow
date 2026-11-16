BEGIN TRAN;

UPDATE g
SET g.createAt = b.primera
FROM gral.factPersonalInfo g
INNER JOIN (SELECT idcorreo, phone, productid, MIN(createAt) AS primera
            FROM gral.factPersonalInfo_bk_20261111
            GROUP BY idcorreo, phone, productid) b
        ON b.idcorreo  = g.idcorreo
       AND b.phone     = g.phone
       AND b.productid = g.productid
WHERE b.primera < g.createAt;

SELECT @@ROWCOUNT AS filas_actualizadas;

COMMIT;

SELECT COUNT(*) AS con_fecha_original
FROM gral.factPersonalInfo
WHERE createAt < '2026-11-11';








SELECT 'fb' AS fuente, COUNT(*) AS vivos, SUM(CASE WHEN countrycode IS NULL THEN 1 ELSE 0 END) AS sin_codigo FROM source.clientsfb WHERE deletedAt IS NULL
UNION ALL SELECT 'bb', COUNT(*), SUM(CASE WHEN countrycode IS NULL THEN 1 ELSE 0 END) FROM source.clientsbb WHERE deletedAt IS NULL
UNION ALL SELECT 'ml', COUNT(*), SUM(CASE WHEN countrycode IS NULL THEN 1 ELSE 0 END) FROM source.clientsml WHERE deletedAt IS NULL
UNION ALL SELECT 'fi', COUNT(*), SUM(CASE WHEN countrycode IS NULL THEN 1 ELSE 0 END) FROM source.clientsfi WHERE deletedAt IS NULL
UNION ALL SELECT 'vc', COUNT(*), SUM(CASE WHEN countrycode IS NULL THEN 1 ELSE 0 END) FROM source.clientsvc WHERE deletedAt IS NULL;


SELECT COUNT(*) AS recuperables
FROM gral.factPersonalInfo g
WHERE g.createAt >= '2026-11-11'
  AND EXISTS (SELECT 1 FROM gral.factPersonalInfo_bk_20261111 b
              WHERE b.idcorreo = g.idcorreo AND b.phone = g.phone
                AND b.productid = g.productid AND b.createAt < g.createAt);



SELECT 'factClientes' AS tabla
     , (SELECT COUNT(*) FROM gral.factClientes) AS ahora
     , (SELECT COUNT(*) FROM gral.factClientes_pre_dom15) AS antes
UNION ALL
SELECT 'factClientesDetalle'
     , (SELECT COUNT(*) FROM gral.factClientesDetalle)
     , (SELECT COUNT(*) FROM gral.factClientesDetalle_pre_dom15)
UNION ALL
SELECT 'factPersonalInfo vivos'
     , (SELECT COUNT(*) FROM gral.factPersonalInfo WHERE deleteAt IS NULL)
     , (SELECT COUNT(*) FROM gral.factPersonalInfo_bk_20261112 WHERE deleteAt IS NULL);




SELECT * INTO gral.factClientes_pre_dom15        FROM gral.factClientes;
SELECT * INTO gral.factClientesDetalle_pre_dom15 FROM gral.factClientesDetalle;

SELECT (SELECT COUNT(*) FROM gral.factClientes)               AS fc_original
     , (SELECT COUNT(*) FROM gral.factClientes_pre_dom15)     AS fc_respaldo
     , (SELECT COUNT(*) FROM gral.factClientesDetalle)               AS fcd_original
     , (SELECT COUNT(*) FROM gral.factClientesDetalle_pre_dom15)     AS fcd_respaldo;


     DROP TABLE source.Phonefb_bk_20261106;
DROP TABLE source.Phonebb_bk_20261109;
DROP TABLE source.Phoneml_bk_20261109;
DROP TABLE source.Phonefi_bk_20261109;
DROP TABLE source.Phonevc_bk_20261109;


-- SQL Server: lo que hizo Gold el domingo
SELECT fecha_ejecucion, nombre_procedimiento, estado, registros_insertados, registros_actualizados
FROM dw_etl.dw_etl_audit_log
WHERE fecha_ejecucion >= '2026-11-15'
ORDER BY fecha_ejecucion;






SELECT s.name AS esquema
     , t.name AS tabla
     , t.create_date
     , (SELECT SUM(p.rows) FROM sys.partitions p
        WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)) AS filas
FROM sys.tables t
INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.name LIKE '%[_]bk[_]%'
   OR t.name LIKE '%[_]bk20%'
   OR t.name LIKE '%prueba%'
   OR t.name LIKE '%test%'
ORDER BY t.create_date;



SELECT TOP 3 fecha_ejecucion, estado, registros_insertados, registros_actualizados, descripcion
FROM dw_etl.dw_etl_audit_log
WHERE nombre_procedimiento = 'sp_insert_phones_factPersonalInfo'
ORDER BY 1 DESC;

SELECT TOP 12 fecha_ejecucion, nombre_procedimiento, estado, registros_insertados, registros_actualizados
FROM dw_etl.dw_etl_audit_log
WHERE nombre_procedimiento <> 'sp_insert_phones_factPersonalInfo'
ORDER BY 1 DESC;



SELECT * INTO gral.factPersonalInfo_bk_20261112 FROM gral.factPersonalInfo;

SELECT (SELECT COUNT(*) FROM gral.factPersonalInfo)             AS original
     , (SELECT COUNT(*) FROM gral.factPersonalInfo_bk_20261112) AS respaldo;


SELECT TOP 8 fecha_ejecucion
     , nombre_procedimiento
     , estado
     , registros_insertados
     , registros_actualizados
FROM dw_etl.dw_etl_audit_log
WHERE nombre_procedimiento <> 'sp_insert_phones_factPersonalInfo'
ORDER BY 1 DESC;



SELECT COUNT(*)         AS retirados_hoy
     , SUM(x.reaparece) AS reaparecen_con_otro_correo
FROM (
    SELECT CASE WHEN EXISTS (SELECT 1 FROM gral.factPersonalInfo n
                             WHERE n.productid = r.productid
                               AND n.phone     = r.phone
                               AND n.deleteAt IS NULL
                               AND n.idcorreo <> r.idcorreo)
                THEN 1 ELSE 0 END AS reaparece
    FROM gral.factPersonalInfo r
    WHERE r.deleteAt >= CAST(SYSDATETIME() AS DATE)
) x;


SELECT TOP 3 fecha_ejecucion, estado, registros_insertados, registros_actualizados, descripcion
FROM dw_etl.dw_etl_audit_log
WHERE nombre_procedimiento = 'sp_insert_phones_factPersonalInfo'
ORDER BY 1 DESC;


SELECT COUNT(*)         AS retirados_hoy
     , SUM(x.reaparece) AS reaparecen_con_otro_correo
FROM (
    SELECT CASE WHEN EXISTS (SELECT 1 FROM gral.factPersonalInfo n
                             WHERE n.productid = r.productid
                               AND n.phone     = r.phone
                               AND n.deleteAt IS NULL
                               AND n.idcorreo <> r.idcorreo)
                THEN 1 ELSE 0 END AS reaparece
    FROM gral.factPersonalInfo r
    WHERE r.deleteAt >= CAST(SYSDATETIME() AS DATE)
) x;



SELECT COUNT(*) AS retirados_hoy
     , SUM(CASE WHEN EXISTS (SELECT 1 FROM gral.factPersonalInfo n
                             WHERE n.productid = r.productid AND n.phone = r.phone
                               AND n.deleteAt IS NULL AND n.idcorreo <> r.idcorreo)
                THEN 1 ELSE 0 END) AS reaparecen_con_otro_correo
FROM gral.factPersonalInfo r
WHERE r.deleteAt >= CAST(SYSDATETIME() AS DATE);



SELECT TOP 15 b.EstatusN AS antes, a.EstatusN AS ahora
     , b.EstatusL AS etiqueta_antes, a.EstatusL AS etiqueta_ahora
     , COUNT(*) AS clientes
FROM gral.factClientesDetalle a
INNER JOIN gral.factClientesDetalle_bk_20261112 b ON b.clientid = a.clientid
WHERE a.clientid % 10 = 1
  AND ISNULL(a.EstatusN, 0) <> ISNULL(b.EstatusN, 0)
GROUP BY b.EstatusN, a.EstatusN, b.EstatusL, a.EstatusL
ORDER BY COUNT(*) DESC;



EXEC dw_etl.sp_insert_phones_factPersonalInfo @simular = 1;




SELECT SUM(CASE WHEN ISNULL(a.country, '')      <> ISNULL(b.country, '')      THEN 1 ELSE 0 END) AS country
     , SUM(CASE WHEN ISNULL(a.countrycode, '')  <> ISNULL(b.countrycode, '')  THEN 1 ELSE 0 END) AS countrycode
     , SUM(CASE WHEN ISNULL(a.state, '')        <> ISNULL(b.state, '')        THEN 1 ELSE 0 END) AS state
     , SUM(CASE WHEN ISNULL(a.city, '')         <> ISNULL(b.city, '')         THEN 1 ELSE 0 END) AS city
     , SUM(CASE WHEN ISNULL(a.zip, '')          <> ISNULL(b.zip, '')          THEN 1 ELSE 0 END) AS zip
     , SUM(CASE WHEN ISNULL(a.corp, '')         <> ISNULL(b.corp, '')         THEN 1 ELSE 0 END) AS corp
     , SUM(CASE WHEN ISNULL(a.corpcode, 0)      <> ISNULL(b.corpcode, 0)      THEN 1 ELSE 0 END) AS corpcode
     , SUM(CASE WHEN ISNULL(a.EstatusL, '')     <> ISNULL(b.EstatusL, '')     THEN 1 ELSE 0 END) AS EstatusL
     , SUM(CASE WHEN ISNULL(a.EstatusN, 0)      <> ISNULL(b.EstatusN, 0)      THEN 1 ELSE 0 END) AS EstatusN
     , SUM(CASE WHEN ISNULL(a.rank, 0)          <> ISNULL(b.rank, 0)          THEN 1 ELSE 0 END) AS rank
     , SUM(CASE WHEN ISNULL(a.NameCompleted, '') <> ISNULL(b.NameCompleted, '') THEN 1 ELSE 0 END) AS nombre
     , SUM(CASE WHEN ISNULL(a.idcorreo, 0)      <> ISNULL(b.idcorreo, 0)      THEN 1 ELSE 0 END) AS idcorreo
     , SUM(CASE WHEN ISNULL(a.deletedAt, '1900-01-01') <> ISNULL(b.deletedAt, '1900-01-01') THEN 1 ELSE 0 END) AS deletedAt
FROM gral.factClientesDetalle a
INNER JOIN gral.factClientesDetalle_bk_20261112 b ON b.clientid = a.clientid
WHERE a.clientid % 10 = 1;




SELECT (SELECT COUNT(*) FROM gral.factClientes)                 AS fc_ahora
     , (SELECT COUNT(*) FROM gral.factClientes_bk_20261112)     AS fc_antes
     , (SELECT COUNT(*) FROM gral.factClientesDetalle)               AS fcd_ahora
     , (SELECT COUNT(*) FROM gral.factClientesDetalle_bk_20261112)   AS fcd_antes;





EXEC dw_etl.sp_etl_maestro;



SELECT * INTO gral.factClientes_bk_20261112        FROM gral.factClientes;
SELECT * INTO gral.factClientesDetalle_bk_20261112 FROM gral.factClientesDetalle;

SELECT (SELECT COUNT(*) FROM gral.factClientes)               AS fc_original
     , (SELECT COUNT(*) FROM gral.factClientes_bk_20261112)   AS fc_respaldo
     , (SELECT COUNT(*) FROM gral.factClientesDetalle)               AS fcd_original
     , (SELECT COUNT(*) FROM gral.factClientesDetalle_bk_20261112)   AS fcd_respaldo;



SELECT o.name AS procedimiento
     , o.modify_date
     , CASE WHEN m.definition LIKE N'%THROW%' THEN 'relanza errores' ELSE 'no relanza' END AS errores
FROM sys.objects o
INNER JOIN sys.sql_modules m ON m.object_id = o.object_id
WHERE o.schema_id = SCHEMA_ID('dw_etl')
  AND o.type = 'P'
  AND o.name LIKE N'sp[_]%'
ORDER BY o.name;



SELECT 1 AS productid
     , SUM(CASE WHEN c.email IS NULL OR TRIM(c.email) = '' OR c.email LIKE '%@noreply.%' THEN 1 ELSE 0 END) AS sin_correo_real
     , SUM(CASE WHEN c.email IS NULL OR TRIM(c.email) = '' OR c.email LIKE '%@noreply.%' THEN 0 ELSE 1 END) AS esperan_gold_clientes
FROM source.Phonefb ph
LEFT JOIN source.clientsfb c ON c.clientid = ph.clientid
WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
  AND NOT EXISTS (SELECT 1 FROM gral.factClientesDetalle fcd WHERE fcd.clientid = ph.clientid * 10 + 1)
UNION ALL
SELECT 2
     , SUM(CASE WHEN c.email IS NULL OR TRIM(c.email) = '' OR c.email LIKE '%@noreply.%' THEN 1 ELSE 0 END)
     , SUM(CASE WHEN c.email IS NULL OR TRIM(c.email) = '' OR c.email LIKE '%@noreply.%' THEN 0 ELSE 1 END)
FROM source.Phonebb ph
LEFT JOIN source.clientsbb c ON c.clientid = ph.clientid
WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
  AND NOT EXISTS (SELECT 1 FROM gral.factClientesDetalle fcd WHERE fcd.clientid = ph.clientid * 10 + 2)
UNION ALL
SELECT 3
     , SUM(CASE WHEN c.email IS NULL OR TRIM(c.email) = '' OR c.email LIKE '%@noreply.%' THEN 1 ELSE 0 END)
     , SUM(CASE WHEN c.email IS NULL OR TRIM(c.email) = '' OR c.email LIKE '%@noreply.%' THEN 0 ELSE 1 END)
FROM source.Phonefi ph
LEFT JOIN source.clientsfi c ON c.clientid = ph.clientid
WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
  AND NOT EXISTS (SELECT 1 FROM gral.factClientesDetalle fcd WHERE fcd.clientid = ph.clientid * 10 + 3)
UNION ALL
SELECT 4
     , SUM(CASE WHEN c.email IS NULL OR TRIM(c.email) = '' OR c.email LIKE '%@noreply.%' THEN 1 ELSE 0 END)
     , SUM(CASE WHEN c.email IS NULL OR TRIM(c.email) = '' OR c.email LIKE '%@noreply.%' THEN 0 ELSE 1 END)
FROM source.Phonevc ph
LEFT JOIN source.clientsvc c ON c.clientid = ph.clientid
WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
  AND NOT EXISTS (SELECT 1 FROM gral.factClientesDetalle fcd WHERE fcd.clientid = ph.clientid * 10 + 4)
UNION ALL
SELECT 5
     , SUM(CASE WHEN c.email IS NULL OR TRIM(c.email) = '' OR c.email LIKE '%@noreply.%' THEN 1 ELSE 0 END)
     , SUM(CASE WHEN c.email IS NULL OR TRIM(c.email) = '' OR c.email LIKE '%@noreply.%' THEN 0 ELSE 1 END)
FROM source.Phoneml ph
LEFT JOIN source.clientsml c ON c.clientid = ph.clientid
WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
  AND NOT EXISTS (SELECT 1 FROM gral.factClientesDetalle fcd WHERE fcd.clientid = ph.clientid * 10 + 5);











SELECT TOP 20 fecha_ejecucion
     , nombre_procedimiento
     , estado
     , registros_insertados
     , registros_actualizados
     , registros_procesados
     , descripcion
FROM dw_etl.dw_etl_audit_log
WHERE nombre_procedimiento <> 'sp_insert_phones_factPersonalInfo'
ORDER BY 1 DESC;




SELECT 1 AS productid, COUNT(*) AS telefonos_esperando
FROM source.Phonefb ph
WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
  AND NOT EXISTS (SELECT 1 FROM gral.factClientesDetalle fcd WHERE fcd.clientid = ph.clientid * 10 + 1)
UNION ALL
SELECT 2, COUNT(*)
FROM source.Phonebb ph
WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
  AND NOT EXISTS (SELECT 1 FROM gral.factClientesDetalle fcd WHERE fcd.clientid = ph.clientid * 10 + 2)
UNION ALL
SELECT 3, COUNT(*)
FROM source.Phonefi ph
WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
  AND NOT EXISTS (SELECT 1 FROM gral.factClientesDetalle fcd WHERE fcd.clientid = ph.clientid * 10 + 3)
UNION ALL
SELECT 4, COUNT(*)
FROM source.Phonevc ph
WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
  AND NOT EXISTS (SELECT 1 FROM gral.factClientesDetalle fcd WHERE fcd.clientid = ph.clientid * 10 + 4)
UNION ALL
SELECT 5, COUNT(*)
FROM source.Phoneml ph
WHERE ph.deletedAt IS NULL AND ph.phone IS NOT NULL AND LTRIM(RTRIM(ph.phone)) <> ''
  AND NOT EXISTS (SELECT 1 FROM gral.factClientesDetalle fcd WHERE fcd.clientid = ph.clientid * 10 + 5);





EXEC dw_etl.sp_insert_phones_factPersonalInfo @simular = 1;

SELECT TOP 5 fecha_ejecucion, estado, registros_insertados, registros_actualizados, descripcion
FROM dw_etl.dw_etl_audit_log
WHERE nombre_procedimiento = 'sp_insert_phones_factPersonalInfo'
ORDER BY 1 DESC;



SELECT TOP 1 clientid, phone
FROM source.Phoneml
WHERE deletedAt IS NULL
ORDER BY clientid;


/*UPDATE source.Phoneml
SET deletedAt = NULL
WHERE clientid = 1 AND phone = '6144880317';*/


SELECT id, idcorreo, phone, createAt, updateAt, deleteAt
FROM gral.factPersonalInfo
WHERE productid = 5 AND phone = '6144880317';


EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 5;


EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 5;




EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 5;



EXEC dw_etl.sp_insert_phones_factPersonalInfo;




EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 1;

EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 4;

EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 3;

SELECT productid
     , COUNT(*)                                              AS filas
     , SUM(CASE WHEN deleteAt IS NULL THEN 1 ELSE 0 END)     AS vivas
FROM gral.factPersonalInfo
GROUP BY productid
ORDER BY productid;


EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 5, @simular = 1;

EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 5;


SELECT COUNT(*) AS parametros
FROM sys.parameters
WHERE object_id = OBJECT_ID(N'dw_etl.sp_insert_phones_factPersonalInfo');


SELECT COUNT(*) AS parametros
FROM sys.parameters
WHERE object_id = OBJECT_ID(N'dw_etl.sp_insert_phones_factPersonalInfo');


SELECT OBJECT_ID(N'dw_etl.sp_insert_phones_factPersonalInfo_bk_20261111') AS existe_copia;

EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 5, @simular = 1;

EXEC dw_etl.sp_insert_phones_factPersonalInfo @productid = 5;

ALTER TABLE gral.factPersonalInfo
    ADD CONSTRAINT UQ_factPersonalInfo_correo_phone_producto
    UNIQUE (idcorreo, phone, productid);

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
ORDER BY o.type_desc, objeto;

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
truncate table gral.factPersonalInfo;

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
