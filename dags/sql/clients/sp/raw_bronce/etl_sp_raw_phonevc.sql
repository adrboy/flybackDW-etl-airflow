CREATE OR REPLACE PROCEDURE `sp_raw_phonevc`(
    IN p_max_pct  DECIMAL(8,4)   -- % maximo de filas vivas que se permite marcar (sugerido: 2)
  , IN p_forzar   TINYINT        -- 1 = ignorar la compuerta
)
BEGIN
  -- Instancia 240. BORRADOR pendiente de pruebas (idempotencia, compuerta, revivir).
  -- Origen: vtw.catowners (telefonos por contrato, worldid) unido a vtw.p_data (operaciones, tradedid)
  -- a traves de db_general.vwpersonalinfovc. clientid = tradedid, por eso un telefono de contrato
  -- genera una fila de RAW por cada operacion del contrato.
  -- La vista se evalua UNA sola vez en una tabla temporal con la llave (clientid, phone);
  -- el insert y el borrado usan esa copia. La vista decide que telefono es valido.
  DECLARE v_inicio       DATETIME     DEFAULT NOW();
  DECLARE v_en_copia     BIGINT       DEFAULT 0;
  DECLARE v_total_antes  BIGINT       DEFAULT 0;
  DECLARE v_borr_antes   BIGINT       DEFAULT 0;
  DECLARE v_total_desp   BIGINT       DEFAULT 0;
  DECLARE v_borr_desp    BIGINT       DEFAULT 0;
  DECLARE v_vivos        BIGINT       DEFAULT 0;
  DECLARE v_por_marcar   BIGINT       DEFAULT 0;
  DECLARE v_marcados     BIGINT       DEFAULT 0;
  DECLARE v_msg          VARCHAR(128);

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    DROP TEMPORARY TABLE IF EXISTS db_general.tmp_phonevc;
    DO RELEASE_LOCK('sp_raw_phonevc');
    RESIGNAL;
  END;

  -- 1) Candado: una sola ejecucion a la vez
  IF GET_LOCK('sp_raw_phonevc', 0) <> 1 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'sp_raw_phonevc ya se esta ejecutando';
  END IF;

  -- 2) Copia de la vista (se evalua una sola vez)
  DROP TEMPORARY TABLE IF EXISTS db_general.tmp_phonevc;
  CREATE TEMPORARY TABLE db_general.tmp_phonevc (
      clientid  BIGINT      NOT NULL
    , phone     VARCHAR(50) NOT NULL
    , PRIMARY KEY (clientid, phone)
  );

  INSERT INTO db_general.tmp_phonevc (clientid, phone)
  SELECT DISTINCT v.clientid
       , v.PHONE
  FROM db_general.vwpersonalinfovc v
  WHERE v.PHONE IS NOT NULL
    AND v.PHONE <> '';

  SELECT COUNT(*) INTO v_en_copia FROM db_general.tmp_phonevc;

  -- Una copia vacia marcaria todo RAW como borrado: se detiene
  IF p_forzar <> 1 AND v_en_copia = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'BLOQUEADO: la vista no devolvio filas';
  END IF;

  -- 3) Cuenta antes
  SELECT COUNT(*)
       , COALESCE(SUM(deletedAt IS NOT NULL), 0)
    INTO v_total_antes
       , v_borr_antes
  FROM db_general.tbl_raw_phonevc;

  -- 4) Insertar nuevos / revivir los que vuelven
  INSERT INTO db_general.tbl_raw_phonevc (clientid, phone)
  SELECT s.clientid
       , s.phone
  FROM db_general.tmp_phonevc s
  ON DUPLICATE KEY UPDATE deletedAt = NULL;

  -- 5) Cuenta despues
  SELECT COUNT(*)
       , COALESCE(SUM(deletedAt IS NOT NULL), 0)
    INTO v_total_desp
       , v_borr_desp
  FROM db_general.tbl_raw_phonevc;

  SET v_vivos = v_total_desp - v_borr_desp;

  -- 6) Compuerta: cuantos se marcarian
  SELECT COUNT(*)
    INTO v_por_marcar
  FROM db_general.tbl_raw_phonevc r
  LEFT JOIN db_general.tmp_phonevc s
         ON s.clientid = r.clientid
        AND s.phone    = r.phone
  WHERE r.deletedAt IS NULL
    AND s.clientid IS NULL;

  IF p_forzar <> 1
     AND v_vivos > 0
     AND v_por_marcar * 100 > v_vivos * p_max_pct THEN
    SET v_msg = CONCAT('BLOQUEADO: marcaria ', v_por_marcar, ' de ', v_vivos
                     , ' vivos (limite ', p_max_pct, '%). Usar p_forzar=1');
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_msg;
  END IF;

  -- 7) Marcar los que ya no existen en el origen
  UPDATE db_general.tbl_raw_phonevc r
  LEFT JOIN db_general.tmp_phonevc s
         ON s.clientid = r.clientid
        AND s.phone    = r.phone
  SET r.deletedAt = NOW()
  WHERE r.deletedAt IS NULL
    AND s.clientid IS NULL;
  SET v_marcados = ROW_COUNT();

  DROP TEMPORARY TABLE IF EXISTS db_general.tmp_phonevc;
  DO RELEASE_LOCK('sp_raw_phonevc');

  -- 8) Resultado
  SELECT v_total_desp - v_total_antes          AS nuevos
       , v_borr_antes - v_borr_desp            AS revividos
       , v_marcados                            AS marcados
       , v_vivos - v_marcados                  AS vivos_final
       , TIMESTAMPDIFF(SECOND, v_inicio, NOW()) AS segundos;
END
