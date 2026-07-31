# ═══════════════════════════════════════════════════════
# run_log_estadisticas.py
# Uso  : python run_log_estadisticas.py
# Desde: etl_datasync/scripts/
# ═══════════════════════════════════════════════════════
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from etl_datasync.operations.log_estadisticas import registrar_estadisticas

if __name__ == '__main__':
    registrar_estadisticas(dag_id='run_manual')
