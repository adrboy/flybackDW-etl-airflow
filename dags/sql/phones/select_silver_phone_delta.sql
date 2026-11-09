-- ═══════════════════════════════════════════════════════
-- SELECT: select_silver_phone_delta.sql
-- Origen : MariaDB — db_general.tbl_raw_phoneXX (fb/bb/ml/fi/vc)
-- Uso    : etl_silver_phone_da.ejecutar_silver_phone()
-- Parametros (se reemplazan en tiempo de ejecucion):
--   raw_tabla — tabla RAW de telefonos del producto
--   marca     — MAX(updatedAt) de Silver, con comillas simples
-- Nota   : >= reprocesa el borde de la marca. Es inofensivo porque
--          el UPDATE y el INSERT de Silver son idempotentes.
--          Trae nuevos, revividos y borrados (deletedAt) por igual.
-- ═══════════════════════════════════════════════════════
SELECT clientid
     , phone
     , updatedAt
     , deletedAt
FROM   {raw_tabla}
WHERE  updatedAt >= {marca}
