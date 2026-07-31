# ═══════════════════════════════════════════════════════
# run_gusa_collections.py
# Uso    : python run_gusa_collections.py
# Desde  : etl_datasync/scripts/
# Hace   : exactamente lo mismo que la task sync_gusa_collections
#          del DAG datasync_master — útil para prueba manual
# ═══════════════════════════════════════════════════════
import sys
import os

# ── Ruta relativa: subimos dos niveles para llegar a /dags ──
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from etl_datasync.operations.gusa_collections import sincronizar_gusa_collections

if __name__ == '__main__':
    sincronizar_gusa_collections(dag_id='run_manual')
