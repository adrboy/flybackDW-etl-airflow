CREATE OR REPLACE PROCEDURE `spAutorizadosDW`()
BEGIN
	##########################################################
	# CREADO POR: ANDRES SARRIA
	# FECHA DE CREACION: 22/05/2023
	# COMENTARIOS: Bloque 1 = mes anterior (upsert).
	#              Bloque 2 = borra lo que ya no existe en origen
	#              e inserta lo faltante (revividos).
	#
	# ACTUALIZADO POR:
	# FECHA:
	# COMENTARIOS:
	#
	###########################################################

	DECLARE v_hasta    INT;
	DECLARE v_origen   INT DEFAULT 0;
	DECLARE v_mes_ant  INT DEFAULT 0;
	DECLARE v_borrados INT DEFAULT 0;
	DECLARE v_nuevos   INT DEFAULT 0;

	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		ROLLBACK;
		DROP TEMPORARY TABLE IF EXISTS tmp_origen_aut;
		RESIGNAL;
	END;

	SET v_hasta = EXTRACT(YEAR_MONTH FROM date_sub(now(), interval 1 month));
	SET lc_time_names = 'es_ES';

	##########################################################
	# BLOQUE 1: MES ANTERIOR (temporal + transaccion + upsert)
	##########################################################
	DROP TEMPORARY TABLE IF EXISTS tmp_origen_aut;
	CREATE TEMPORARY TABLE tmp_origen_aut AS
	SELECT A.indice                                AS idautorizado
		 , EXTRACT(YEAR FROM B.inicio_r)           AS Y_inicio
		 , D.idcorp
		 , D.iddev
		 , B.redeem_no
		 , coalesce(C.country,'OTHER')             AS pais
		 , EXTRACT(YEAR FROM A.f_authorized)       AS Y_autho
		 , EXTRACT(YEAR_MONTH FROM A.f_authorized) AS AM_autho
		 , EXTRACT(MONTH FROM A.f_authorized)      AS MMN_autho
		 , UPPER(MONTHNAME(A.f_authorized))        AS MML_autho
		 , date(A.f_authorized)                    AS D_autho
		 , CASE WHEN MONTH(A.f_authorized) IN (1,2,3)    THEN 1
				WHEN MONTH(A.f_authorized) IN (4,5,6)    THEN 2
				WHEN MONTH(A.f_authorized) IN (7,8,9)    THEN 3
				ELSE 4 END                         AS TRIM
	FROM customers.pago_redeem A
	INNER JOIN customers.redeems B    ON B.pagoid  = A.indice
	LEFT JOIN  customers.fb_clients C ON C.clientid = B.clientid
	INNER JOIN customers.develops D   ON D.iddev   = C.company
	WHERE EXTRACT(YEAR_MONTH FROM A.f_authorized) = v_hasta
	GROUP BY A.indice;

	SELECT COUNT(1) INTO v_origen FROM tmp_origen_aut;

	IF v_origen = 0 THEN
		SIGNAL SQLSTATE '45000'
			SET MESSAGE_TEXT = 'spAutorizadosDW: origen del mes anterior vacio, no se toca el DW';
	END IF;

	START TRANSACTION;

		DELETE FROM flybackDW.factredeemsautorizados
		WHERE AM_autho = v_hasta;

		INSERT INTO flybackDW.factredeemsautorizados
			(idautorizado,Y_inicio,idcorp,iddev,redeem_no,pais,
			 Y_autho,AM_autho,MMN_autho,MML_autho,D_autho,TRIM)
		SELECT idautorizado,Y_inicio,idcorp,iddev,redeem_no,pais,
			   Y_autho,AM_autho,MMN_autho,MML_autho,D_autho,TRIM
		FROM tmp_origen_aut
		ORDER BY Y_inicio, AM_autho, idcorp
		ON DUPLICATE KEY
		UPDATE Y_inicio  = VALUES(Y_inicio)
			 , idcorp    = VALUES(idcorp)
			 , iddev     = VALUES(iddev)
			 , redeem_no = VALUES(redeem_no)
			 , pais      = VALUES(pais)
			 , Y_autho   = VALUES(Y_autho)
			 , AM_autho  = VALUES(AM_autho)
			 , MMN_autho = VALUES(MMN_autho)
			 , MML_autho = VALUES(MML_autho)
			 , D_autho   = VALUES(D_autho)
			 , TRIM      = VALUES(TRIM);
		SET v_mes_ant = v_origen;

	COMMIT;

	DROP TEMPORARY TABLE IF EXISTS tmp_origen_aut;

	##########################################################
	# BLOQUE 2: LIMPIEZA + REVIVIDOS (historico hasta mes anterior)
	##########################################################
	START TRANSACTION;

		-- 2.1) Borrar del DW todo lo que ya no existe en origen (sin filtro de fecha)
		DELETE F
		FROM flybackDW.factredeemsautorizados F
		WHERE NOT EXISTS (
				SELECT 1
				FROM customers.pago_redeem A
				INNER JOIN customers.redeems B    ON B.pagoid  = A.indice
				LEFT JOIN  customers.fb_clients C ON C.clientid = B.clientid
				INNER JOIN customers.develops D   ON D.iddev   = C.company
				WHERE A.indice = F.idautorizado
				  AND A.f_authorized IS NOT NULL
		  );
		SET v_borrados = ROW_COUNT();

		-- 2.2) Insertar lo que existe en origen y NO existe en el DW (revividos)
		INSERT INTO flybackDW.factredeemsautorizados
			(idautorizado,Y_inicio,idcorp,iddev,redeem_no,pais,
			 Y_autho,AM_autho,MMN_autho,MML_autho,D_autho,TRIM)
		SELECT A.indice
			 , EXTRACT(YEAR FROM B.inicio_r)
			 , D.idcorp
			 , D.iddev
			 , B.redeem_no
			 , coalesce(C.country,'OTHER')
			 , EXTRACT(YEAR FROM A.f_authorized)
			 , EXTRACT(YEAR_MONTH FROM A.f_authorized)
			 , EXTRACT(MONTH FROM A.f_authorized)
			 , UPPER(MONTHNAME(A.f_authorized))
			 , date(A.f_authorized)
			 , CASE WHEN MONTH(A.f_authorized) IN (1,2,3)    THEN 1
					WHEN MONTH(A.f_authorized) IN (4,5,6)    THEN 2
					WHEN MONTH(A.f_authorized) IN (7,8,9)    THEN 3
					ELSE 4 END
		FROM customers.pago_redeem A
		INNER JOIN customers.redeems B    ON B.pagoid  = A.indice
		LEFT JOIN  customers.fb_clients C ON C.clientid = B.clientid
		INNER JOIN customers.develops D   ON D.iddev   = C.company
		WHERE A.f_authorized IS NOT NULL
		  AND EXTRACT(YEAR_MONTH FROM A.f_authorized) <= v_hasta
		  AND NOT EXISTS (
				SELECT 1
				FROM flybackDW.factredeemsautorizados F
				WHERE F.idautorizado = A.indice)
		GROUP BY A.indice;
		SET v_nuevos = ROW_COUNT();

	COMMIT;

	SELECT v_hasta    AS hasta,
		   v_mes_ant  AS filas_mes_anterior,
		   v_borrados AS borrados_del_historico,
		   v_nuevos   AS revividos_o_faltantes;
END