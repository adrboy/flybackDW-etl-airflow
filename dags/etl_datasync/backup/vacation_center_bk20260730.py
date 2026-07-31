# ═══════════════════════════════════════════════════════
# datasync/operations/vacation_center.py  — BACKUP 2026-07-30
# Origen   : 192.168.10.240  (vtw)
# Destino  : 192.168.10.242  (db_general)
# ═══════════════════════════════════════════════════════
import traceback
from airflow.providers.mysql.hooks.mysql import MySqlHook

CONN_VTC    = 'MariaDB240'
CONN_GLOBAL = 'MariaDB'

SQL_TRUNCATE     = "TRUNCATE db_general.vtw;"
SQL_LOG_TRUNCATE = "INSERT INTO db_general.log(description) VALUES ('TRUNCATE vtw');"

SQL_SELECT = """
    SELECT
          1 AS update_id
        , REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(b.worldid),'.',''),')',''),'(',''),'-',''),' ',''),'+','') AS contract
        , COALESCE(UPPER(CONCAT(b.fname1,' ',b.lname1)),'') AS client
        , COALESCE(UPPER(CONCAT(b.fname2,' ',b.lname2)),'') AS clientTwo
        , COALESCE(UPPER(b.fname1),'') AS fname1
        , COALESCE(UPPER(b.lname1),'') AS lname1
        , COALESCE(UPPER(b.fname2),'') AS fname2
        , COALESCE(UPPER(b.lname2),'') AS lname2
        , COALESCE(UPPER(b.email1),'') AS email
        , COALESCE(UPPER(b.email2),'') AS emailTwo
        , COALESCE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(b.ph11,')',''),'(',''),'-',''),' ',''),'+',''),'') AS telephone
        , 1 AS vtw
        , IFNULL((SELECT iso_iii FROM CatPaises.cat_pais
                  WHERE Pais=b.country OR pais_in=b.country OR pais_es=b.country LIMIT 1),'OTH') AS country_code
        , COALESCE(b.country,'') AS country
        , COALESCE(b.state,'')   AS state
        , COALESCE(a.iddev,'')   AS dev
        , COALESCE(crp.corpname,'') AS corp
        , COALESCE(DATE_FORMAT(a.capdata,'%Y-%m-%d'),'0001-01-01') AS capdata
        , COALESCE(SUM(IF(p.statusn IN (2,3,8), p.monto, 0)),0)    AS fee
        , COALESCE(cs.stslet,'') AS status
    FROM vtw.p_data a
    LEFT JOIN vtw.catowners        b   ON a.worldid  = b.worldid
    INNER JOIN vtw.controlstatusvtw cs ON a.statusn  = cs.statusn AND a.substatus = cs.substatus
    LEFT JOIN vtw.catdevelopers    dev ON a.iddev     = dev.iddev
    LEFT JOIN vtw.catcorpsdev      crp ON dev.idcorp  = crp.idcorp
    LEFT JOIN vtw.payments         p   ON a.tradedid  = p.tradedid
    GROUP BY contract, client
"""

SQL_INSERT = """
    INSERT INTO db_general.vtw (
          update_id, contract, client, clientTwo
        , fname1, lname1, fname2, lname2
        , email, emailTwo, telephone, vtw
        , country_code, country, state, dev, corp
        , capdata, fee, status
    ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s
        , %s, %s, %s, %s, %s, %s, %s, %s, %s
        , IF(%s<'1000-01-01',NULL,%s), %s, %s
    )
"""

SQL_LOG_INSERT = "INSERT INTO db_general.log(description) VALUES ('insert vtw');"


def sincronizar_vacation_center(dag_id: str) -> int:
    hook_origen  = MySqlHook(mysql_conn_id=CONN_VTC)
    hook_destino = MySqlHook(mysql_conn_id=CONN_GLOBAL)
    conn_origen  = None
    conn_destino = None
    try:
        conn_origen  = hook_origen.get_conn()
        conn_destino = hook_destino.get_conn()
        conn_destino.autocommit = False
        cur_origen  = conn_origen.cursor()
        cur_destino = conn_destino.cursor()
        print(f"[{dag_id}] vacation_center — truncando tabla...")
        cur_destino.execute(SQL_TRUNCATE)
        cur_destino.execute(SQL_LOG_TRUNCATE)
        print(f"[{dag_id}] vacation_center — leyendo vtw db (240)...")
        cur_origen.execute(SQL_SELECT)
        filas = cur_origen.fetchall()
        total = len(filas)
        print(f"[{dag_id}] vacation_center — {total:,} registros leídos")
        filas_preparadas = []
        for f in filas:
            fila    = list(f)
            capdata = fila[17]
            fila_final = fila[:17] + [capdata, capdata] + fila[18:]
            filas_preparadas.append(fila_final)
        cur_destino.executemany(SQL_INSERT, filas_preparadas)
        cur_destino.execute(SQL_LOG_INSERT)
        conn_destino.commit()
        print(f"[{dag_id}] vacation_center — OK ✅  ({total:,} filas)")
        return total
    except Exception:
        if conn_destino:
            conn_destino.rollback()
        print(f"[{dag_id}] vacation_center — ERROR ❌\n{traceback.format_exc()}")
        raise
    finally:
        if conn_origen:  conn_origen.close()
        if conn_destino: conn_destino.close()
