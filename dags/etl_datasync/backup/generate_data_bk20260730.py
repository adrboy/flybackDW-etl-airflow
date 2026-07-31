# ═══════════════════════════════════════════════════════
# datasync/operations/generate_data.py  — BACKUP 2026-07-30
# Objetivo : Consolidar GC + FB + BB + VTW → db_general.complete
# Servidor : 192.168.10.242  (db_general — todo interno)
# ═══════════════════════════════════════════════════════
import traceback
from airflow.providers.mysql.hooks.mysql import MySqlHook

CONN_GLOBAL = 'MariaDB'

SQL_TRUNCATE     = "TRUNCATE db_general.complete;"
SQL_LOG_TRUNCATE = "INSERT INTO db_general.log(description) VALUES ('TRUNCATE complete');"

SQL_INSERT_COMPLETE = """
    INSERT INTO db_general.complete (
          update_id, client, clientTwo, fname1, lname1, fname2, lname2
        , contract, email, emailTwo, telephone, country_code
        , gc, fb, bb, vtw, ml, b, pw, rw, total
        , gc_n_b, gc_n_pw, gc_n_rw
        , gc_country, gc_state, gc_dev, gc_corp
        , gc_credit, gc_pay_number, gc_paid_number, gc_paid, gc_balance, gc_currency, gc_sign, gc_status
        , fb_ncert, fb_vcert, fb_sign, fb_activated, fb_years, fb_currency, fb_status
        , fb_country, fb_state, fb_dev, fb_corp
        , bb_ncert, bb_vcert, bb_sign, bb_activated, bb_years, bb_currency, bb_status
        , bb_country, bb_state, bb_dev, bb_corp
        , vtw_capdata, vtw_fee, vtw_status, vtw_country, vtw_state, vtw_dev, vtw_corp
    )
    SELECT DISTINCT 1 update_id
        , client
        , GROUP_CONCAT(DISTINCT IF(clientTwo='',NULL,clientTwo)) clientTwo
        , fname1, lname1, fname2, lname2, contract
        , MAX(email) email, MAX(emailTwo) emailTwo, MAX(telephone) telephone
        , GROUP_CONCAT(DISTINCT IF(country_code='OTH',NULL,country_code)) country_code
        , SUM(gc), SUM(fb), SUM(bb), SUM(vtw), SUM(ml), SUM(b), SUM(pw), SUM(rw)
        , SUM(gc+fb+bb+vtw+ml+b+pw+rw) total
        , SUM(gc_n_b), SUM(gc_n_pw), SUM(gc_n_rw)
        , GROUP_CONCAT(gc_country), GROUP_CONCAT(gc_state), GROUP_CONCAT(gc_dev), GROUP_CONCAT(gc_corp)
        , GROUP_CONCAT(gc_credit), GROUP_CONCAT(gc_pay_number), GROUP_CONCAT(gc_paid_number)
        , GROUP_CONCAT(gc_paid), GROUP_CONCAT(gc_balance), GROUP_CONCAT(gc_currency)
        , GROUP_CONCAT(gc_sign), GROUP_CONCAT(gc_status)
        , GROUP_CONCAT(fb_ncert), GROUP_CONCAT(fb_vcert), GROUP_CONCAT(fb_sign), GROUP_CONCAT(fb_activated)
        , GROUP_CONCAT(fb_years), GROUP_CONCAT(fb_currency), GROUP_CONCAT(fb_status)
        , GROUP_CONCAT(fb_country), GROUP_CONCAT(fb_state), GROUP_CONCAT(fb_dev), GROUP_CONCAT(fb_corp)
        , GROUP_CONCAT(bb_ncert), GROUP_CONCAT(bb_vcert), GROUP_CONCAT(bb_sign), GROUP_CONCAT(bb_activated)
        , GROUP_CONCAT(bb_years), GROUP_CONCAT(bb_currency), GROUP_CONCAT(bb_status)
        , GROUP_CONCAT(bb_country), GROUP_CONCAT(bb_state), GROUP_CONCAT(bb_dev), GROUP_CONCAT(bb_corp)
        , GROUP_CONCAT(vtw_capdata), GROUP_CONCAT(vtw_fee), GROUP_CONCAT(vtw_status)
        , GROUP_CONCAT(vtw_country), GROUP_CONCAT(vtw_state), GROUP_CONCAT(vtw_dev), GROUP_CONCAT(vtw_corp)
    FROM (
        SELECT gc.client, gc.clientTwo, gc.fname1, gc.lname1, gc.fname2, gc.lname2
             , gc.contract, gc.email, gc.emailTwo, gc.telephone, gc.country_code
             , gc.gc, 0 fb, 0 bb, 0 vtw, 0 ml
             , IF(bpw.b IS NULL,0,IF(bpw.b>0,1,0)) b
             , IF(bpw.pw IS NULL,0,IF(bpw.pw>0,1,0)) pw
             , IF(bpw.rw IS NULL,0,IF(bpw.rw>0,1,0)) rw
             , 0 total, bpw.b gc_n_b, bpw.pw gc_n_pw, bpw.rw gc_n_rw
             , gc.country gc_country, gc.state gc_state, gc.dev gc_dev, gc.corp gc_corp
             , gc.credit gc_credit, gc.pay_number gc_pay_number, gc.paid_number gc_paid_number
             , gc.paid gc_paid, gc.balance gc_balance, gc.currency gc_currency
             , gc.sign gc_sign, gc.status gc_status
             , NULL fb_ncert, NULL fb_vcert, NULL fb_sign, NULL fb_activated
             , NULL fb_years, NULL fb_currency, NULL fb_status, NULL fb_country
             , NULL fb_state, NULL fb_dev, NULL fb_corp
             , NULL bb_ncert, NULL bb_vcert, NULL bb_sign, NULL bb_activated
             , NULL bb_years, NULL bb_currency, NULL bb_status, NULL bb_country
             , NULL bb_state, NULL bb_dev, NULL bb_corp
             , NULL vtw_capdata, NULL vtw_fee, NULL vtw_status
             , NULL vtw_country, NULL vtw_state, NULL vtw_dev, NULL vtw_corp
        FROM db_general.gusa_collections gc
        LEFT JOIN db_general.beyond_pw bpw
               ON gc.contract = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(bpw.contract),')',''),'(',''),'-',''),' ',''),'+','')
        UNION
        SELECT fb.client, fb.clientTwo, fb.fname1, fb.lname1, fb.fname2, fb.lname2
             , fb.contract, fb.email, fb.emailTwo, fb.telephone, fb.country_code
             , 0, fb.fb, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
             , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
             , fb.ncert, fb.vcert, fb.sign, fb.activated, fb.years, fb.currency, fb.status
             , fb.country, fb.state, fb.dev, fb.corp
             , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
             , NULL, NULL, NULL, NULL, NULL, NULL, NULL
        FROM db_general.flyback fb
        UNION
        SELECT bb.client, bb.clientTwo, bb.fname1, bb.lname1, bb.fname2, bb.lname2
             , bb.contract, bb.email, bb.emailTwo, bb.telephone, bb.country_code
             , 0, 0, bb.bb, 0, 0, 0, 0, 0, 0, 0, 0, 0
             , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
             , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
             , bb.ncert, bb.vcert, bb.sign, bb.activated, bb.years, bb.currency, bb.status
             , bb.country, bb.state, bb.dev, bb.corp
             , NULL, NULL, NULL, NULL, NULL, NULL, NULL
        FROM db_general.buyback bb
        UNION
        SELECT vtw.client, vtw.clientTwo, vtw.fname1, vtw.lname1, vtw.fname2, vtw.lname2
             , vtw.contract, vtw.email, vtw.emailTwo, vtw.telephone, vtw.country_code
             , 0, 0, 0, vtw.vtw, 0, 0, 0, 0, 0, 0, 0, 0
             , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
             , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
             , NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
             , vtw.capdata, vtw.fee, vtw.status, vtw.country, vtw.state, vtw.dev, vtw.corp
        FROM db_general.vtw vtw
    ) t
    GROUP BY contract, client
    ORDER BY total DESC
"""


def generar_data(dag_id: str) -> None:
    hook = MySqlHook(mysql_conn_id=CONN_GLOBAL)
    conn = None
    try:
        conn   = hook.get_conn()
        cursor = conn.cursor()
        conn.autocommit = False
        print(f"[{dag_id}] generate_data — truncando complete...")
        cursor.execute(SQL_TRUNCATE)
        cursor.execute(SQL_LOG_TRUNCATE)
        print(f"[{dag_id}] generate_data — consolidando GC + FB + BB + VTW...")
        cursor.execute(SQL_INSERT_COMPLETE)
        conn.commit()
        print(f"[{dag_id}] generate_data — OK ✅")
    except Exception:
        if conn:
            conn.rollback()
        print(f"[{dag_id}] generate_data — ERROR ❌\n{traceback.format_exc()}")
        raise
    finally:
        if conn:
            conn.close()
