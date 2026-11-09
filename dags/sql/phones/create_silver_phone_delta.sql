-- ═══════════════════════════════════════════════════════
-- CREATE: create_silver_phone_delta.sql
-- Destino: SQL Server — tabla temporal de la sesion
-- Uso    : etl_silver_phone_da.ejecutar_silver_phone()
-- Nota   : #delta vive solo en la conexion de la transaccion;
--          desaparece sola al cerrarla. La llave (clientid, phone)
--          acelera el UPDATE y el INSERT por conjuntos.
-- Collation: las tablas temporales nacen en tempdb y heredan SU
--          collation (SQL_Latin1_General_CP1_CI_AS), distinta de la de
--          la base (Modern_Spanish_CI_AS). COLLATE DATABASE_DEFAULT hace
--          que phone use la collation de la base y el JOIN con
--          source.PhoneXX no falle (error 468).
-- ═══════════════════════════════════════════════════════
CREATE TABLE #delta (
    clientid  INT           NOT NULL
  , phone     VARCHAR(30)   COLLATE DATABASE_DEFAULT NOT NULL
  , updatedAt DATETIME2(7)  NOT NULL
  , deletedAt DATETIME2(7)  NULL
  , PRIMARY KEY (clientid, phone)
)
