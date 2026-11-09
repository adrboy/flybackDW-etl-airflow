-- ═══════════════════════════════════════════════════════
-- UPDATE: update_silver_phone.sql
-- Destino: SQL Server — source.PhoneXX (fb/bb/ml/fi/vc)
-- Parametro: tabla_destino — se reemplaza en tiempo de ejecucion
-- Uso    : etl_silver_phone_da.ejecutar_silver_phone()
-- Nota   : actualiza SOLO lo que cambio. deletedAt se copia tal
--          cual desde RAW, SIN COALESCE: si RAW lo pone en NULL
--          (telefono que vuelve al origen), Silver tambien lo
--          revive. Idempotente: segunda corrida = 0 filas.
-- ═══════════════════════════════════════════════════════
UPDATE t
SET    t.updatedAt = d.updatedAt
     , t.deletedAt = d.deletedAt
FROM   {tabla_destino} AS t
INNER JOIN #delta AS d
        ON d.clientid = t.clientid
       AND d.phone    = t.phone
WHERE  t.updatedAt <> d.updatedAt
    OR ISNULL(t.deletedAt, '1900-01-01') <> ISNULL(d.deletedAt, '1900-01-01')
