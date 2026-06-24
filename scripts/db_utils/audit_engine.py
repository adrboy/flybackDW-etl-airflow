"""
audit_engine.py
Motor de Auditoría Genérico — compara MAX(id) origen vs destino.
Lee sync_config.json como única fuente de verdad.
Ejecucion manual desde PowerShell:
    python scripts/db_utils/audit_engine.py
Futuro: migrar conexiones a Airflow Hooks y mover a dags/common/
Fecha: 2026-06-24
"""

import os
import json
import time
import pymysql
from datetime import datetime
from dotenv   import load_dotenv

# ── Cargar variables de entorno ──────────────────────────
load_dotenv(dotenv_path=r"C:\Users\GUSA CAPITAL\Documents\DockersETL\.env")

MARIADB_HOST = os.environ["MARIADB_SOURCE_HOST"]
MARIADB_USER = os.environ["MARIADB_SOURCE_USER"]
MARIADB_PASS = os.environ["MARIADB_SOURCE_PASS"]
MARIADB_PORT = int(os.environ["MARIADB_SOURCE_PORT"])

CONFIG_PATH  = os.path.join(os.path.dirname(__file__), "sync_config.json")


# ── Logger centralizado ──────────────────────────────────
# Hoy: print en PowerShell
# Futuro: tabla flybackDW.etl_audit_log
def log(nivel: str, task_id: str, mensaje: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{nivel.upper():7s}] [{task_id}] {mensaje}")


# ── Conexión MariaDB genérica ────────────────────────────
def get_mariadb_conn(database: str):
    return pymysql.connect(
        host     = MARIADB_HOST
       ,user     = MARIADB_USER
       ,password = MARIADB_PASS
       ,port     = MARIADB_PORT
       ,database = database
    )


# ── Motor de Auditoría ───────────────────────────────────
class AuditMonitor:
    """
    Agnóstico — recibe el objeto JSON completo.
    Si mañana agregas campos extra al JSON, esta clase no se rompe.
    Ambas tablas (origen y destino) están en MariaDB.
    """

    def __init__(self, tarea: dict):
        self.tarea       = tarea
        self.task_id     = tarea.get("task_id",     "unknown")
        self.descripcion = tarea.get("descripcion", "")
        self.dag_id      = tarea.get("dag_id",      "")
        self.origen      = tarea.get("origen",      {})
        self.destino     = tarea.get("destino",     {})
        self.max_origen  = None
        self.max_destino = None
        self.diferencia  = None
        self.estado      = None
        self.error       = None

    def _get_max(self, conn, tabla: str, id_col: str) -> int:
        sql    = f"SELECT COALESCE(MAX({id_col}), 0) FROM {tabla}"
        cursor = conn.cursor()
        cursor.execute(sql)
        return cursor.fetchone()[0]

    def ejecutar(self) -> dict:
        log("INFO", self.task_id, f"Iniciando — {self.descripcion}")
        t0 = time.perf_counter()

        conn_origen  = None
        conn_destino = None

        try:
            # ── MAX origen ───────────────────────────────
            conn_origen     = get_mariadb_conn(self.origen["database"])
            self.max_origen = self._get_max(
                conn_origen
               ,self.origen["tabla"]
               ,self.origen["id_col"]
            )
            log("INFO", self.task_id, f"MAX origen  ({self.origen['tabla']}.{self.origen['id_col']}) : {self.max_origen:,}")

            # ── MAX destino ──────────────────────────────
            conn_destino     = get_mariadb_conn(self.destino["database"])
            self.max_destino = self._get_max(
                conn_destino
               ,self.destino["tabla"]
               ,self.destino["id_col"]
            )
            log("INFO", self.task_id, f"MAX destino ({self.destino['tabla']}.{self.destino['id_col']}) : {self.max_destino:,}")

            # ── Comparación ──────────────────────────────
            self.diferencia = self.max_origen - self.max_destino

            if self.diferencia > 0:
                self.estado = "DIFERENCIA"
                log("WARN", self.task_id,
                    f"⚠️  {self.diferencia:,} registros pendientes — DAG [{self.dag_id}] debe ejecutarse")
            else:
                self.estado = "SINCRONIZADO"
                log("INFO", self.task_id,
                    f"✅ Sincronizado — sin diferencias")

        except Exception as e:
            # Resiliencia — continúa con la siguiente tarea
            self.estado = "ERROR"
            self.error  = str(e)
            log("ERROR", self.task_id, f"❌ {self.error}")

        finally:
            if conn_origen  is not None: conn_origen.close()
            if conn_destino is not None: conn_destino.close()

        t1 = time.perf_counter()
        log("INFO", self.task_id, f"Tiempo: {t1 - t0:.2f}s | Estado: {self.estado}")

        return {
            "task_id"     : self.task_id
           ,"descripcion" : self.descripcion
           ,"dag_id"      : self.dag_id
           ,"max_origen"  : self.max_origen
           ,"max_destino" : self.max_destino
           ,"diferencia"  : self.diferencia
           ,"estado"      : self.estado
           ,"error"       : self.error
        }


# ── Orquestador principal ────────────────────────────────
def main():
    print("=" * 60)
    print("  audit_engine.py — Motor de Auditoría Genérico")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Leer catálogo
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        catalogo = json.load(f)

    log("INFO", "engine", f"Catálogo cargado — {len(catalogo)} tarea(s)")

    resultados      = []
    con_diferencias = []

    # Iterar — si una falla continúa con la siguiente
    for tarea in catalogo:
        print()
        monitor   = AuditMonitor(tarea)
        resultado = monitor.ejecutar()
        resultados.append(resultado)

        if resultado["estado"] == "DIFERENCIA":
            con_diferencias.append(resultado)

    # ── Resumen final ────────────────────────────────────
    print()
    print("=" * 60)
    print("  RESUMEN FINAL")
    print("=" * 60)
    for r in resultados:
        icono = "⚠️ " if r["estado"] == "DIFERENCIA"  else \
                "✅ " if r["estado"] == "SINCRONIZADO" else "❌ "
        dif   = f"{r['diferencia']:,}" if r["diferencia"] is not None else "N/A"
        print(f"  {icono} {r['task_id']:20s} | diferencia: {dif:>10s} | {r['estado']}")

    print()
    if con_diferencias:
        print("  DAGs que requieren ejecución:")
        for r in con_diferencias:
            log("WARN", "engine",
                f"▶ airflow dags trigger {r['dag_id']}")
    else:
        log("INFO", "engine", "Todo sincronizado ✅ — ningún DAG requiere ejecución")

    print("=" * 60)


if __name__ == "__main__":
    main()
