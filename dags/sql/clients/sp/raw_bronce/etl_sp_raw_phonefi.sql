CREATE OR REPLACE PROCEDURE `sp_raw_phonefi`(
    IN p_max_pct  DECIMAL(8,4)   -- % maximo de filas vivas que se permite marcar (sugerido: 2)
  , IN p_forzar   TINYINT        -- 1 = ignorar la compuerta
)
BEGIN
  -- Instancia 240. BORRADOR pendiente de pruebas (idempotencia, compuerta, revivir).
  -- Origen: financiamiento.clients_phone (number BIGINT) a traves de db_general.vwpersonalinfofi.
  -- La vista decide que telefono es valido (rango de digitos, active); este procedimiento solo la sigue.
  -- RAW guarda el numero como texto; la comparacion se hace como numero (RAW -> UNSIGNED)
  -- porque en fi el RAW solo contiene digitos y asi se evitan conflictos de collation.
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
    DO RELEASE_LOCK('sp_raw_phonefi');
    RESIGNAL;
  END;

  -- 1) Candado: una sola ejecucion a la vez
  IF GET_LOCK('sp_raw_phonefi', 0) <> 1 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'sp_raw_phonefi ya se esta ejecutando';
  END IF;

  -- 2) Cuenta antes
  SELECT COUNT(*)
       , COALESCE(SUM(deletedAt IS NOT NULL), 0)
    INTO v_total_antes
       , v_borr_antes
  FROM db_general.tbl_raw_phonefi;

  -- 3) Insertar nuevos / revivir los que vuelven
  INSERT INTO db_general.tbl_raw_phonefi (clientid, phone)
  SELECT v.clientid
       , CAST(v.PHONE AS CHAR)
  FROM db_general.vwpersonalinfofi v
  WHERE v.PHONE IS NOT NULL
    AND v.PHONE > 0
  ON DUPLICATE KEY UPDATE deletedAt = NULL;

  -- 4) Cuenta despues
  SELECT COUNT(*)
       , COALESCE(SUM(deletedAt IS NOT NULL), 0)
    INTO v_total_desp
       , v_borr_desp
  FROM db_general.tbl_raw_phonefi;

  SET v_vivos = v_total_desp - v_borr_desp;

  -- 5) Compuerta: cuantos se marcarian
  SELECT COUNT(*)
    INTO v_por_marcar
  FROM db_general.tbl_raw_phonefi r
  WHERE r.deletedAt IS NULL
    AND NOT EXISTS (
        SELECT 1
        FROM db_general.vwpersonalinfofi v
        WHERE v.clientid = r.clientid
          AND v.PHONE    = CAST(r.phone AS UNSIGNED)
    );

  IF p_forzar <> 1
     AND v_vivos > 0
     AND v_por_marcar * 100 > v_vivos * p_max_pct THEN
    SET v_msg = CONCAT('BLOQUEADO: marcaria ', v_por_marcar, ' de ', v_vivos
                     , ' vivos (limite ', p_max_pct, '%). Usar p_forzar=1');
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = v_msg;
  END IF;

  -- 6) Marcar los que ya no existen en el origen
  UPDATE db_general.tbl_raw_phonefi r
  SET r.deletedAt = NOW()
  WHERE r.deletedAt IS NULL
    AND NOT EXISTS (
        SELECT 1
        FROM db_general.vwpersonalinfofi v
        WHERE v.clientid = r.clientid
          AND v.PHONE    = CAST(r.phone AS UNSIGNED)
    );
  SET v_marcados = ROW_COUNT();

  DO RELEASE_LOCK('sp_raw_phonefi');

  -- 7) Resultado
  SELECT v_total_desp - v_total_antes          AS nuevos
       , v_borr_antes - v_borr_desp            AS revividos
       , v_marcados                            AS marcados
       , v_vivos - v_marcados                  AS vivos_final
       , TIMESTAMPDIFF(SECOND, v_inicio, NOW()) AS segundos;
END
