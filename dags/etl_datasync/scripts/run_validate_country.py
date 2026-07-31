# ═══════════════════════════════════════════════════════
# run_validate_country.py
# Uso    : python run_validate_country.py
# Desde  : etl_datasync/scripts/
# Hace   : exactamente lo mismo que la task validate_country
#          del DAG datasync_master — útil para prueba manual
# Nota   : requiere que complete ya esté poblada
# ═══════════════════════════════════════════════════════
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from etl_datasync.operations.validate_country import validar_country

if __name__ == '__main__':
    validar_country(dag_id='run_manual')
