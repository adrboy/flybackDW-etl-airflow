-- ═══════════════════════════════════════════════════════
-- INSERT: insert_silver_phone_delta.sql
-- Destino: SQL Server — #delta (tabla temporal de la sesion)
-- Uso    : etl_silver_phone_da.ejecutar_silver_phone()
-- Nota   : placeholders para pyodbc (? en lugar de %s)
--          las fechas llegan como texto (o NULL) y se convierten aqui;
--          asi fast_executemany no falla con parametros nulos
-- ═══════════════════════════════════════════════════════
INSERT INTO #delta
       ( clientid, phone, updatedAt, deletedAt)
VALUES ( ?, ?, CAST(? AS DATETIME2(7)), CAST(? AS DATETIME2(7)))
