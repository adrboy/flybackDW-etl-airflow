# ═══════════════════════════════════════════════════════
# datasync/operations/gusa_collections.py
# Objetivo : Sincronizar clientes GC → db_general.gusa_collections
# Origen   : 192.168.10.240  (financiamiento)
# Destino  : 192.168.10.242  (db_general)
# Versión  : 1.1 — 2026-07-30
# ═══════════════════════════════════════════════════════
import traceback
from airflow.providers.mysql.hooks.mysql import MySqlHook

# Conexiones definidas en common/db_connections.py
CONN_GC     = 'MariaDB240'   # financiamiento — origen  (servidor 240)
CONN_GLOBAL = 'MariaDB'      # db_general     — destino (servidor 242)

SQL_TRUNCATE = "TRUNCATE db_general.gusa_collections;"

SQL_LOG_TRUNCATE = "INSERT INTO db_general.log(description) VALUES ('truncate gusa_collections');"

SQL_SELECT = """
    SELECT DISTINCT
          1                                                                          AS update_id
        , UPPER(CONCAT(c.fname1, ' ', c.lname1))                                   AS client
        , UPPER(CONCAT(c.fname2, ' ', c.lname2))                                   AS clientTwo
        , IFNULL(UPPER(c.fname1), '')                                               AS fname1
        , IFNULL(UPPER(c.lname1), '')                                               AS lname1
        , IFNULL(UPPER(c.fname2), '')                                               AS fname2
        , IFNULL(UPPER(c.lname2), '')                                               AS lname2
        , (IF(SUBSTRING(cr.contract,1,2)='D-',
              SUBSTRING(cr.contract,3),
              IF(SUBSTRING(cr.contract,LENGTH(cr.contract))='R',
                 SUBSTRING(cr.contract,1,LENGTH(cr.contract)-1),
                 cr.contract)))                                                      AS contract
        , IFNULL(UPPER(ce.email), '')                                               AS email
        , IFNULL(UPPER(ce.emailTwo), '')                                            AS emailTwo
        , IFNULL(UPPER(cp.number), '')                                              AS telephone
        , 1                                                                         AS gc
        , IFNULL(
            (SELECT iso_iii FROM CatPaises.cat_pais
             WHERE Pais = cps.NOMBRE OR pais_in = cps.NOMBRE OR pais_es = cps.NOMBRE
             LIMIT 1), 'OTH')                                                       AS country_code
        , IFNULL(cps.NOMBRE, '')                                                    AS country
        , IFNULL(c.state, '')                                                       AS state
        , cr.deve                                                                   AS dev
        , csdc.corpname                                                             AS corp
        , MAX(cr.fin_amount)                                                        AS credit
        , MAX(cr.num_pay)                                                           AS pay_number
        , COUNT(DISTINCT CASE WHEN cc.statusc IN (5,6,7,14,16) THEN cc.idx END)    AS paid_number
        , IFNULL(SUM(
            CASE WHEN cc.amprogmxp > 0
                 THEN IFNULL(cc.pay_amount, 0)
                 ELSE cc.pay_amount END), 0)                                        AS paid
        , IFNULL(MAX(ccta.saldo_final), 0)                                         AS balance
        , MAX(cr.fund_cur)                                                          AS currency
        , MAX(cr.sig_date)                                                          AS sign
        , MAX(cscr.name_cr)                                                         AS status
    FROM financiamiento.clients c
    LEFT JOIN financiamiento.credits cr          ON c.client_id = cr.client_id
    LEFT JOIN (SELECT client_id, MAX(email) email, MIN(email) emailTwo
               FROM financiamiento.clients_email GROUP BY client_id)  ce ON ce.client_id = c.client_id
    LEFT JOIN (SELECT client_id, MAX(number) number
               FROM financiamiento.clients_phone GROUP BY client_id)  cp ON cp.client_id = c.client_id
    LEFT JOIN financiamiento.credits_collec_ta   ccta ON c.client_id = ccta.client_id AND ccta.estatus = 1
    LEFT JOIN financiamiento.credits_collec      cc   ON c.client_id = cc.client_id AND cc.statusc IN (5,6,7,14,16)
    LEFT JOIN financiamiento.cat_pais            cps  ON c.country   = cps.CODIGO
    LEFT JOIN financiamiento.cat_status_cr       cscr ON cr.status_c = cscr.id_status_cr
    LEFT JOIN financiamiento.cat_develops        csd  ON cr.deve      = csd.deve
    LEFT JOIN financiamiento.cat_devcorps        csdc ON csd.idCorp   = csdc.idCorp
    WHERE cr.contract IS NOT NULL
    GROUP BY client, contract
"""

SQL_INSERT = """
    INSERT INTO db_general.gusa_collections (
          update_id, client, clientTwo
        , fname1, lname1, fname2, lname2
        , contract, email, emailTwo, telephone
        , gc, country_code, country, state
        , dev, corp, credit, pay_number
        , paid_number, paid, balance
        , currency, sign, status
    ) VALUES (
          %s, %s, %s
        , %s, %s, %s, %s
        , %s, %s, %s, %s
        , %s, %s, %s, %s
        , %s, %s, %s, %s
        , %s, %s, %s
        , %s, %s, %s
    )
"""

SQL_LOG_INSERT = "INSERT INTO db_general.log(description) VALUES ('Insert gusa_collections');"


def sincronizar_gusa_collections(dag_id: str) -> int:
    """
    Lee financiamiento (240) → trunca e inserta en db_general.gusa_collections (242).
    Retorna el número de filas insertadas.
    """
    hook_origen  = MySqlHook(mysql_conn_id=CONN_GC)
    hook_destino = MySqlHook(mysql_conn_id=CONN_GLOBAL)

    conn_origen  = None
    conn_destino = None

    try:
        conn_origen  = hook_origen.get_conn()
        conn_destino = hook_destino.get_conn()
        conn_destino.autocommit = False

        cur_origen  = conn_origen.cursor()
        cur_destino = conn_destino.cursor()

        print(f"[{dag_id}] gusa_collections — truncando tabla...")
        cur_destino.execute(SQL_TRUNCATE)
        cur_destino.execute(SQL_LOG_TRUNCATE)

        print(f"[{dag_id}] gusa_collections — leyendo financiamiento (240)...")
        cur_origen.execute(SQL_SELECT)
        filas = cur_origen.fetchall()
        total = len(filas)
        print(f"[{dag_id}] gusa_collections — {total:,} registros leídos")

        cur_destino.executemany(SQL_INSERT, filas)
        cur_destino.execute(SQL_LOG_INSERT)

        conn_destino.commit()
        print(f"[{dag_id}] gusa_collections — OK ✅  ({total:,} filas)")
        return total

    except Exception:
        if conn_destino:
            conn_destino.rollback()
        print(f"[{dag_id}] gusa_collections — ERROR ❌\n{traceback.format_exc()}")
        raise

    finally:
        if conn_origen:  conn_origen.close()
        if conn_destino: conn_destino.close()
