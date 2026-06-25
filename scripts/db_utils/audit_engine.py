"""
audit_engine.py
Motor de Auditoria Generica — compara COUNT y MAX(id) origen vs destino.
Lee sync_config.json como unica fuente de verdad.
Ejecucion manual desde PowerShell:
    python scripts/db_utils/audit_engine.py
Futuro: migrar conexiones a Airflow Hooks y mover a dags/common/
Fecha: 2026-06-25 v2.0 — sql_count personalizado por tarea
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
def log(nivel: str, task_id: str, mensaje: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{nivel.upper():7s}] [{task_id}] {mensaje}")


# ── Conexion MariaDB generica ────────────────────────────
def get_mariadb_conn(database: str):
    return pymysql.connect(
        host     = MARIADB_HOST
       ,user     = MARIADB_USER
       ,password = MARIADB_PASS
       ,port     = MARIADB_PORT
       ,database = database
    )


# ── Motor de Auditoria ───────────────────────────────────
class AuditMonitor:
    """
    Agnostico — recibe el objeto JSON completo.
    Soporta sql_count personalizado para respetar el universo del SP.
    Si el objeto tiene 'sql_count' lo usa — si no, usa MAX(id_col) simple.
    """

    def __init__(self, tarea: dict):
        self.tarea       = tarea
        self.task_id     = tarea.get("task_id",     "unknown")
        self.descripcion = tarea.get("descripcion", "")
        self.dag_id      = tarea.get("dag_id",      "")
        self.origen      = tarea.get("origen",      {})
        self.destino     = tarea.get("destino",     {})
        self.count_origen  = None
        self.count_destino = None
        self.max_origen    = None
        self.max_destino   = None
        self.diferencia    = None
        self.estado        = None
        self.error         = None

    def _get_count_max(self, conn, sql_count: str) -> tuple:
        """Ejecuta sql_count — debe retornar COUNT(*), MAX(id)"""
        cursor = conn.cursor()
        cursor.execute(sql_count)
        row = cursor.fetchone()
        return int(row[0] or 0), int(row[1] or 0)

    def _get_max_simple(self, conn, tabla: str, id_col: str) -> int:
        sql    = f"SELECT COALESCE(MAX({id_col}), 0) FROM {tabla}"
        cursor = conn.cursor()
        cursor.execute(sql)
        return int(cursor.fetchone()[0] or 0)

    def ejecutar(self) -> dict:
        log("INFO", self.task_id, f"Iniciando — {self.descripcion}")
        t0 = time.perf_counter()

        conn_origen  = None
        conn_destino = None

        try:
            conn_origen  = get_mariadb_conn(self.origen["database"])
            conn_destino = get_mariadb_conn(self.destino["database"])

            # ── Origen ───────────────────────────────────
            if "sql_count" in self.origen:
                self.count_origen, self.max_origen = self._get_count_max(
                    conn_origen, self.origen["sql_count"]
                )
            else:
                self.count_origen = None
                self.max_origen   = self._get_max_simple(
                    conn_origen, self.origen["tabla"], self.origen["id_col"]
                )
            log("INFO", self.task_id,
                f"Origen  COUNT={self.count_origen:,} | MAX({self.origen['id_col']})={self.max_origen:,}")

            # ── Destino ──────────────────────────────────
            if "sql_count" in self.destino:
                self.count_destino, self.max_destino = self._get_count_max(
                    conn_destino, self.destino["sql_count"]
                )
            else:
                self.count_destino = None
                self.max_destino   = self._get_max_simple(
                    conn_destino, self.destino["tabla"], self.destino["id_col"]
                )
            log("INFO", self.task_id,
                f"Destino COUNT={self.count_destino:,} | MAX({self.destino['id_col']})={self.max_destino:,}")

            # ── Comparacion por COUNT ────────────────────
            if self.count_origen is not None and self.count_destino is not None:
                self.diferencia = self.count_origen - self.count_destino
            else:
                self.diferencia = self.max_origen - self.max_destino

            if self.diferencia > 0:
                self.estado = "DIFERENCIA"
                log("WARN", self.task_id,
                    f"Diferencia: {self.diferencia:,} registros pendientes — DAG [{self.dag_id}] debe ejecutarse")
            else:
                self.estado = "SINCRONIZADO"
                log("INFO", self.task_id, f"Sincronizado — sin diferencias")

        except Exception as e:
            self.estado = "ERROR"
            self.error  = str(e)
            log("ERROR", self.task_id, f"{self.error}")

        finally:
            if conn_origen  is not None: conn_origen.close()
            if conn_destino is not None: conn_destino.close()

        t1 = time.perf_counter()
        log("INFO", self.task_id, f"Tiempo: {t1 - t0:.2f}s | Estado: {self.estado}")

        return {
            "task_id"      : self.task_id
           ,"descripcion"  : self.descripcion
           ,"dag_id"       : self.dag_id
           ,"count_origen" : self.count_origen
           ,"count_destino": self.count_destino
           ,"max_origen"   : self.max_origen
           ,"max_destino"  : self.max_destino
           ,"diferencia"   : self.diferencia
           ,"estado"       : self.estado
           ,"error"        : self.error
        }


# ── Orquestador principal ────────────────────────────────
def main():
    print("=" * 60)
    print("  audit_engine.py v2.0 — Motor de Auditoria Generica")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        catalogo = json.load(f)

    log("INFO", "engine", f"Catalogo cargado — {len(catalogo)} tarea(s)")

    resultados      = []
    con_diferencias = []

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
        cnt_o = f"{r['count_origen']:,}"  if r["count_origen"]  is not None else "N/A"
        cnt_d = f"{r['count_destino']:,}" if r["count_destino"] is not None else "N/A"
        print(f"  {icono} {r['task_id']:20s} | count origen: {cnt_o:>10s} | count destino: {cnt_d:>10s} | diff: {dif:>8s} | {r['estado']}")

    print()
    if con_diferencias:
        print("  DAGs que requieren ejecucion:")
        for r in con_diferencias:
            log("WARN", "engine", f"airflow dags trigger {r['dag_id']}")
    else:
        log("INFO", "engine", "Todo sincronizado — ningun DAG requiere ejecucion")

    print("=" * 60)


if __name__ == "__main__":
    main()
