CREATE OR REPLACE PROCEDURE `sp_raw_phonebb`(
    IN p_max_pct  DECIMAL(8,4)   -- % maximo de filas vivas que se permite marcar (sugerido: 2)
  , IN p_forzar   TINYINT        -- 1 = ignorar la compuerta
)
BEGIN
  -- Instancia 242. BORRADOR pendiente de pruebas (idempotencia, compuerta, revivir).
  -- Origen: buyback.clients (ph11, ph12) a traves de db_general.vwpersonalinfobb.
  -- Misma logica que sp_raw_phonefb: el borrado va directo contra la tabla de origen.
  DECLARE v_inicio       DATETIME     DEFAULT NOW();
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
    DO RELEASE_LOCK('sp_raw_phonebb');
    RESIGNAL;
  END;

  -- 1) Candado: una sola ejecucion a la vez
  IF GET_LOCK('sp_raw_phonebb', 0) <> 1 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'sp_raw_phonebb ya se esta ejecutando';
  END IF;

  -- 2) Cuenta antes
  SELECT COUNT(*)
       , COALESCE(SUM(deletedAt IS NOT NULL), 0)
    INTO v_total_antes
       , v_borr_antes
  FROM db_general.tbl_raw_phonebb;

  -- 3) Insertar nuevos / revivir los que vuelven
  INSERT INTO db_general.tbl_raw_phonebb (clientid, phone)
  SELECT v.clientid
       , v.PHONE
  FROM db_general.vwpersonalinfobb v
  WHERE v.PHONE IS NOT NULL
    AND v.PHONE <> ''
  ON DUPLICATE KEY UPDATE deletedAt = NULL;

  -- 4) Cuenta despues
  SELECT COUNT(*)
       , COALESCE(SUM(deletedAt IS NOT NULL), 0)
    INTO v_total_desp
       , v_borr_desp
  FROM db_general.tbl_raw_phonebb;

  SET v_vivos = v_total_desp - v_borr_desp;

  -- 5) Compuerta: cuantos se marcarian
  SELECT COUNT(*)
    INTO v_por_marcar
  FROM db_general.tbl_raw_phonebb r
  WHERE r.deletedAt IS NULL
    AND NOT EXISTS (
        SELECT 1
        FROM buyback.clients c
        WHERE c.clientid = r.clientid
          AND r.phone IN (c.ph11, c.ph12)
    );

  IF p_forzar <> 1
     AND v_vivos > 0
     AND v_por_marcar * 100 > v_vivos * p_max_pct THEN
    SET v_msg = CONCAT('BLOQUEADO: marcaria ', v_por_marcar, ' de ', v_vivos
                     , ' vivos (limite ', p_max_pct, '%). Usar p_forzar=1');
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_msg;
  END IF;

  -- 6) Marcar los que ya no existen en el origen
  UPDATE db_general.tbl_raw_phonebb r
  SET r.deletedAt = NOW()
  WHERE r.deletedAt IS NULL
    AND NOT EXISTS (
        SELECT 1
        FROM buyback.clients c
        WHERE c.clientid = r.clientid
          AND r.phone IN (c.ph11, c.ph12)
    );
  SET v_marcados = ROW_COUNT();

  DO RELEASE_LOCK('sp_raw_phonebb');

  -- 7) Resultado
  SELECT v_total_desp - v_total_antes          AS nuevos
       , v_borr_antes - v_borr_desp            AS revividos
       , v_marcados                            AS marcados
       , v_vivos - v_marcados                  AS vivos_final
       , TIMESTAMPDIFF(SECOND, v_inicio, NOW()) AS segundos;
END
