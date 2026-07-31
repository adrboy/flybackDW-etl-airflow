# ═══════════════════════════════════════════════════════
# run_buyback.py
# Uso  : python run_buyback.py
# Desde: etl_datasync/scripts/
# Hace : exactamente lo mismo que la task sync_buyback del DAG
# ═══════════════════════════════════════════════════════
import sys
import os

# Sube dos niveles: scripts/ → etl_datasync/ → dags/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from etl_datasync.operations.buyback import sincronizar_buyback

if __name__ == '__main__':
    sincronizar_buyback(dag_id='run_manual')
