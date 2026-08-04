# ═══════════════════════════════════════════════════════
# datasync/operations/flyback.py
# Objetivo : Sincronizar clientes FB → db_general.flyback
# Origen   : 192.168.10.240  (customers)
# Destino  : 192.168.10.242  (db_general)
# Versión  : 1.0 — 2026-07-29
# ═══════════════════════════════════════════════════════
import traceback
from airflow.providers.mysql.hooks.mysql import MySqlHook

CONN_FB     = 'MariaDB240'   # customers — origen
CONN_GLOBAL = 'MariaDB'      # db_general — destino

SQL_TRUNCATE   = "TRUNCATE db_general.flyback;"
SQL_LOG_TRUNCATE = "INSERT INTO db_general.log(description) VALUES ('Truncate flyback');"

SQL_SELECT = """
    WITH X AS (
        SELECT
              1                                                                                          AS update_id
            , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(fb.contractid),')',''),'(',''),'-',''),' ',''),'+','')  AS contract
            , IFNULL(UPPER(CONCAT(fb.fname1,' ',fb.lname1)),'')                                         AS client
            , IFNULL(UPPER(CONCAT(fb.fname2,' ',fb.lname2)),'')                                         AS clientTwo
            , IFNULL(UPPER(fb.fname1),'')                                                               AS fname1
            , IFNULL(UPPER(fb.lname1),'')                                                               AS lname1
            , IFNULL(UPPER(fb.fname2),'')                                                               AS fname2
            , IFNULL(UPPER(fb.lname2),'')                                                               AS lname2
            , IFNULL(UPPER(IF(fb.ema1='' OR fb.ema1 IS NULL,
                              IF(fb.ema2='' OR fb.ema2 IS NULL, fb.ema3, fb.ema2),
                              fb.ema1)),'')                                                              AS email
            , IFNULL(UPPER(IF(fb.ema2='' OR fb.ema2 IS NULL, fb.ema3, fb.ema2)),'')                    AS emailTwo
            , IFNULL(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(fb.ph11),')',''),'(',''),'-',''),' ',''),'+',''),'') AS telephone
            , 1                                                                                         AS fb
            , IFNULL((SELECT iso_iii FROM CatPaises.cat_pais
                      WHERE Pais=fb.country OR pais_in=fb.country OR pais_es=fb.country
                      LIMIT 1),'OTH')                                                                   AS country_code
            , IFNULL(fb.country,'')                                                                     AS country
            , IFNULL(fb.state,'')                                                                       AS state
            , IFNULL(fb.company,'')                                                                     AS dev
            , (SELECT corpname FROM customers.devcorps
               WHERE corplevel = (SELECT x.idcorp FROM customers.develops x WHERE x.iddev=fb.company)) AS corp
            , IFNULL(SUM(IF(fb.contractid IS NOT NULL,1,0)),0)                                         AS ncert
            , IFNULL(SUM(IF(fb.venta>=9000,
                            fb.venta / (SELECT AVG(exchange_rate) FROM customers.cat_date
                                        WHERE anno=YEAR(fb.capdata)),
                            fb.venta)),0)                                                               AS vcert
            , IFNULL(fb.signdate,'0001-01-01')                                                          AS sign
            , IFNULL(fb.inicio_r,'0001-01-01')                                                          AS activated
            , IFNULL((SELECT MAX(t.redeem_no) FROM customers.redeems t
                      WHERE t.contractid=fb.contractid),0)                                              AS years
            , fb.currency                                                                               AS currency
            , (SELECT e.descrip FROM customers.estatus e WHERE e.idstatus=fb.status)                   AS status
        FROM customers.fb_clients fb
        WHERE NOT ISNULL(fb.contractid)
        GROUP BY fb.clientid
    )
    SELECT update_id, contract, client, clientTwo
         , fname1, lname1, fname2, lname2
         , email, emailTwo, telephone, fb
         , country_code, country, state
         , dev, corp, ncert, vcert
         , sign, activated, years, currency, status
    FROM X
    GROUP BY client, contract
"""

SQL_INSERT = """
    INSERT INTO db_general.flyback (
          update_id, contract, client, clientTwo
        , fname1, lname1, fname2, lname2
        , email, emailTwo, telephone, fb
        , country_code, country, state
        , dev, corp, ncert, vcert
        , sign, activated, years, currency, status
    ) VALUES (
          %s, %s, %s, %s
        , %s, %s, %s, %s
        , %s, %s, %s, %s
        , %s, %s, %s
        , %s, %s, %s, %s
        , IF(%s<'1000-01-01',NULL,%s), IF(%s<'1000-01-01',NULL,%s)
        , %s, %s, %s
    )
"""

SQL_LOG_INSERT = "INSERT INTO db_general.log(description) VALUES ('Insert flyback');"


def sincronizar_flyback(dag_id: str) -> int:
    hook_origen  = MySqlHook(mysql_conn_id=CONN_FB)
    hook_destino = MySqlHook(mysql_conn_id=CONN_GLOBAL)
    conn_origen  = None
    conn_destino = None
    try:
        conn_origen  = hook_origen.get_conn()
        conn_destino = hook_destino.get_conn()
        conn_destino.autocommit = False
        cur_origen  = conn_origen.cursor()
        cur_destino = conn_destino.cursor()
        print(f"[{dag_id}] flyback — truncando tabla...")
        cur_destino.execute(SQL_TRUNCATE)
        cur_destino.execute(SQL_LOG_TRUNCATE)
        print(f"[{dag_id}] flyback — leyendo customers (240)...")
        cur_origen.execute(SQL_SELECT)
        filas = cur_origen.fetchall()
        total = len(filas)
        print(f"[{dag_id}] flyback — {total:,} registros leídos")
        filas_preparadas = []
        for f in filas:
            fila = list(f)
            sign      = fila[19]
            activated = fila[20]
            fila_final = fila[:19] + [sign, sign, activated, activated] + fila[21:]
            filas_preparadas.append(fila_final)
        cur_destino.executemany(SQL_INSERT, filas_preparadas)
        cur_destino.execute(SQL_LOG_INSERT)
        conn_destino.commit()
        print(f"[{dag_id}] flyback — OK ✅  ({total:,} filas)")
        return total
    except Exception:
        if conn_destino:
            conn_destino.rollback()
        print(f"[{dag_id}] flyback — ERROR ❌\n{traceback.format_exc()}")
        raise
    finally:
        if conn_origen:  conn_origen.close()
        if conn_destino: conn_destino.close()
