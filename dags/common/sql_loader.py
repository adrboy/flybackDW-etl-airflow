# ═══════════════════════════════════════════════════════
# sql_loader.py
# Objetivo: Cargar archivos .sql externos dinámicamente
# Carpeta: common/
# Versión: 1.1 — 2026-06-19 (fix BASE_DIR → dags/)
# ═══════════════════════════════════════════════════════
# USO:
#   from common.sql_loader import cargar_sql
#   query = cargar_sql('sql/clients/select_clientsbb_242.sql')
#   query = cargar_sql('sql/clients/select_clientsbb_242.sql', max_id=1000)
# ═══════════════════════════════════════════════════════

import os

# Ruta raíz desde common/ → subimos un nivel → dags/
# /opt/airflow/dags/common/sql_loader.py → /opt/airflow/dags/
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')
)

def cargar_sql(ruta_relativa: str, **params) -> str:
    """
    Carga un archivo .sql desde la carpeta sql/ dentro de dags/.

    Args:
        ruta_relativa: Ruta relativa desde dags/.
                       Ejemplo: 'sql/clients/select_clientsbb_242.sql'
        **params:      Parámetros opcionales para reemplazar en el SQL.
                       Ejemplo: cargar_sql('select.sql', max_id=1000)

    Returns:
        String con el contenido del archivo .sql,
        con los parámetros reemplazados si se proporcionaron.

    Raises:
        FileNotFoundError: Si el archivo .sql no existe.
    """
    ruta_completa = os.path.join(BASE_DIR, ruta_relativa)

    if not os.path.exists(ruta_completa):
        raise FileNotFoundError(
            f"Archivo SQL no encontrado: {ruta_completa}\n"
            f"BASE_DIR: {BASE_DIR}\n"
            f"Ruta relativa: {ruta_relativa}"
        )

    with open(ruta_completa, 'r', encoding='utf-8') as f:
        sql = f.read()

    # Reemplazar parámetros si se proporcionaron
    # Ejemplo: {max_id} en el .sql → max_id=1000 en el llamado
    if params:
        sql = sql.format(**params)

    return sql


if __name__ == "__main__":
    # ── Test de humo ─────────────────────────────────────
    print(f"BASE_DIR: {BASE_DIR}")
    try:
        sql = cargar_sql('sql/clients/select_clientsbb_242.sql', max_id=99999)
        print("✅ select_clientsbb_242.sql cargado OK")
        print(sql)
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
