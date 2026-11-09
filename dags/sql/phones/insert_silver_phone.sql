-- ═══════════════════════════════════════════════════════
-- INSERT: insert_silver_phone.sql
-- Destino: SQL Server — source.PhoneXX (fb/bb/ml/fi/vc)
-- Parametro: tabla_destino — se reemplaza en tiempo de ejecucion
-- Uso    : etl_silver_phone_da.ejecutar_silver_phone()
-- Nota   : inserta SOLO lo que falta (NOT EXISTS por llave
--          clientid + phone). createdAt toma su valor por defecto
--          (SYSDATETIME) = cuando la fila llego a Silver.
--          Idempotente: segunda corrida = 0 filas.
-- ═══════════════════════════════════════════════════════
INSERT INTO {tabla_destino}
       ( clientid, phone, updatedAt, deletedAt)
SELECT d.clientid
     , d.phone
     , d.updatedAt
     , d.deletedAt
FROM   #delta AS d
WHERE  NOT EXISTS (
           SELECT 1
           FROM   {tabla_destino} AS t
           WHERE  t.clientid = d.clientid
             AND  t.phone    = d.phone
       )
