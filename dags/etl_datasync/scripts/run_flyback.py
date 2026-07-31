# ═══════════════════════════════════════════════════════
# run_flyback.py
# Uso    : python run_flyback.py
# Desde  : etl_datasync/scripts/
# Hace   : exactamente lo mismo que la task sync_flyback
#          del DAG datasync_master — útil para prueba manual
# ═══════════════════════════════════════════════════════
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from etl_datasync.operations.flyback import sincronizar_flyback

if __name__ == '__main__':
    sincronizar_flyback(dag_id='run_manual')
