# ═══════════════════════════════════════════════════════
# run_generate_data.py
# Uso    : python run_generate_data.py
# Desde  : etl_datasync/scripts/
# Hace   : exactamente lo mismo que la task generate_data
#          del DAG datasync_master — útil para prueba manual
# Nota   : requiere que las 4 tablas fuente ya estén pobladas
#          (gusa_collections, flyback, buyback, vtw)
# ═══════════════════════════════════════════════════════
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from etl_datasync.operations.generate_data import generar_data

if __name__ == '__main__':
    generar_data(dag_id='run_manual')
