# ═══════════════════════════════════════════════════════
# datasync/operations/buyback.py  — BACKUP 2026-07-30
# Origen   : 192.168.10.242  (buyback)
# Destino  : 192.168.10.242  (db_general)
# ═══════════════════════════════════════════════════════
import traceback
from airflow.providers.mysql.hooks.mysql import MySqlHook

CONN_BB     = 'MariaDB240'
CONN_GLOBAL = 'MariaDB'

SQL_TRUNCATE     = "TRUNCATE db_general.buyback;"
SQL_LOG_TRUNCATE = "INSERT INTO db_general.log(description) VALUES ('truncate buyback');"

SQL_SELECT = """
    SELECT
          1                                                                                              AS update_id
        , IFNULL(UPPER(CONCAT(c.fname1,' ',c.lname1)),'')                                              AS client
        , IFNULL(UPPER(CONCAT(c.fname2,' ',c.lname2)),'')                                              AS clientTwo
        , IFNULL(UPPER(c.fname1),'')                                                                    AS fname1
        , IFNULL(UPPER(c.lname1),'')                                                                    AS lname1
        , IFNULL(UPPER(c.fname2),'')                                                                    AS fname2
        , IFNULL(UPPER(c.lname2),'')                                                                    AS lname2
        , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(c.contractid),')',''),'(',''),'-',''),' ',''),'+','') AS contract
        , IFNULL(UPPER(IF(c.ema1='' OR c.ema1 IS NULL, IF(c.ema2='' OR c.ema2 IS NULL, c.ema2, c.ema2), c.ema1)),'') AS email
        , IFNULL(UPPER(IF(c.ema2='' OR c.ema2 IS NULL, c.ema2, c.ema2)),'')                            AS emailTwo
        , IFNULL(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(c.ph11),')',''),'(',''),'-',''),' ',''),'+',''),'') AS telephone
        , 1                                                                                             AS bb
        , IFNULL((SELECT iso_iii FROM CatPaises.cat_pais
                  WHERE Pais=c.country OR pais_in=c.country OR pais_es=c.country LIMIT 1),'OTH')       AS country_code
        , IFNULL(c.country,'')  AS country
        , IFNULL(c.state,'')    AS state
        , IFNULL(c.company,'')  AS dev
        , IFNULL(crp.corpname,'') AS corp
        , COUNT(*)              AS ncert
        , IFNULL(SUM(c.income),0) AS vcert
        , IFNULL(c.signdate,'0001-01-01')    AS sign
        , IFNULL(c.factivacion,'0001-01-01') AS activated
        , IFNULL(c.period,0)    AS years
        , 'USD'                 AS currency
        , GROUP_CONCAT(DISTINCT sb.descrip) AS status
    FROM buyback.clients c
    LEFT JOIN buyback.estatus  sb  ON sb.idstatus = c.status
    LEFT JOIN buyback.develops dev ON c.company   = dev.iddev
    LEFT JOIN buyback.devcorps crp ON dev.idcorp  = crp.corplevel
    GROUP BY client, contract
"""

SQL_INSERT = """
    INSERT INTO db_general.buyback (
          update_id, contract, client, clientTwo
        , fname1, lname1, fname2, lname2
        , email, emailTwo, telephone, bb
        , country_code, country, state
        , dev, corp, ncert, vcert
        , sign, activated, years, currency, status
    ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s
        , %s, %s, %s, %s, %s, %s, %s
        , %s, %s, %s, %s
        , IF(%s<'1000-01-01',NULL,%s), IF(%s<'1000-01-01',NULL,%s)
        , %s, %s, %s
    )
"""

SQL_LOG_INSERT = "INSERT INTO db_general.log(description) VALUES ('insert buyback');"


def sincronizar_buyback(dag_id: str) -> int:
    hook_origen  = MySqlHook(mysql_conn_id=CONN_BB)
    hook_destino = MySqlHook(mysql_conn_id=CONN_GLOBAL)
    conn_origen  = None
    conn_destino = None
    try:
        conn_origen  = hook_origen.get_conn()
        conn_destino = hook_destino.get_conn()
        conn_destino.autocommit = False
        cur_origen  = conn_origen.cursor()
        cur_destino = conn_destino.cursor()
        print(f"[{dag_id}] buyback — truncando tabla...")
        cur_destino.execute(SQL_TRUNCATE)
        cur_destino.execute(SQL_LOG_TRUNCATE)
        print(f"[{dag_id}] buyback — leyendo buyback db (242)...")
        cur_origen.execute(SQL_SELECT)
        filas = cur_origen.fetchall()
        total = len(filas)
        print(f"[{dag_id}] buyback — {total:,} registros leídos")
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
        print(f"[{dag_id}] buyback — OK ✅  ({total:,} filas)")
        return total
    except Exception:
        if conn_destino:
            conn_destino.rollback()
        print(f"[{dag_id}] buyback — ERROR ❌\n{traceback.format_exc()}")
        raise
    finally:
        if conn_origen:  conn_origen.close()
        if conn_destino: conn_destino.close()
