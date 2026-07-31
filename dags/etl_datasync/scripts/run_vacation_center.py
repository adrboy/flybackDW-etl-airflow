# ═══════════════════════════════════════════════════════
# run_vacation_center.py
# Uso    : python run_vacation_center.py
# Desde  : etl_datasync/scripts/
# Hace   : exactamente lo mismo que la task sync_vacation_center
#          del DAG datasync_master — útil para prueba manual
# ═══════════════════════════════════════════════════════
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from etl_datasync.operations.vacation_center import sincronizar_vacation_center

if __name__ == '__main__':
    sincronizar_vacation_center(dag_id='run_manual')
