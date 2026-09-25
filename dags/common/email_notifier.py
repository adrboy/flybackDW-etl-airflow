# ═══════════════════════════════════════════════════════
# email_notifier.py
# Objetivo: Enviar notificación por email con
#           el contenido de un log ETL
# Carpeta : common/
# Versión : 2.0 — 2026-09-24
#   v1.0: lee el reporte desde archivo log_path
#   v2.0: acepta contenido en memoria (contenido=)
#         log_path → solo fallback si contenido no se pasa
#         contenido en memoria → sin dependencia de disco
# ═══════════════════════════════════════════════════════
import smtplib
import os
from email.mime.text      import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime             import datetime

SMTP_HOST     = "mail.gusacapital.com"
SMTP_PORT     = 587
SMTP_USER     = os.getenv("EMAIL_USER",     "andres@gusacapital.com")
SMTP_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
SMTP_FROM     = "andres@gusacapital.com"


def send_etl_notification(
    dag_id     : str
  , status     : str
  , contenido  : str  = None   # ← reporte en memoria (preferido)
  , log_path   : str  = None   # ← fallback: ruta al archivo .txt
  , recipients : list = None
  , extra_msg  : str  = None
) -> bool:
    """
    Envía un email con el resultado de un proceso ETL.

    Prioridad del contenido del cuerpo:
      1. contenido  → reporte en memoria, sin leer disco ✅
      2. log_path   → lee el archivo si contenido no se pasó
      3. extra_msg  → mensaje adicional siempre incluido

    Parámetros:
        dag_id    : Nombre del DAG
        status    : 'OK' o 'ERROR'
        contenido : Reporte en memoria — preferido, sin dependencia de disco
        log_path  : Ruta al archivo .txt — solo si contenido es None
        recipients: Lista de emails destino
        extra_msg : Texto adicional opcional
    """
    if recipients is None:
        recipients = ["andres@gusacapital.com"]

    # ── Resolver contenido del cuerpo ─────────────────────
    # Prioridad: contenido en memoria > archivo en disco
    log_content = ""

    if contenido is not None:
        # Reporte en memoria — sin leer disco ✅
        log_content = contenido

    elif log_path is not None:
        # Fallback: leer desde archivo
        if os.path.isfile(log_path):
            try:
                with open(log_path, "r", encoding="utf-8") as f:
                    log_content = f.read()
            except Exception as e:
                log_content = f"[AVISO] No se pudo leer el archivo: {e}"
        else:
            log_content = f"[AVISO] Archivo no encontrado: {log_path}"

    # ── Construir asunto y cuerpo ─────────────────────────
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    emoji     = "✅" if status == "OK" else "❌"
    subject   = f"{emoji} ETL {dag_id} — {status} — {timestamp}"

    body = (
        f"ETL Notification — Gusacapital\n"
        f"{'='*50}\n\n"
        f"Proceso  : {dag_id}\n"
        f"Estado   : {status}\n"
        f"Fecha    : {timestamp}\n\n"
        f"{'='*50}\n"
    )

    if extra_msg:
        body += f"\nDetalle:\n{extra_msg}\n\n{'='*50}\n"

    if log_content:
        body += f"\nReporte:\n{'-'*50}\n{log_content}"
    else:
        body += "\n[Sin contenido de reporte]"

    # ── Construir y enviar mensaje MIME ───────────────────
    msg            = MIMEMultipart()
    msg["From"]    = SMTP_FROM
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, recipients, msg.as_string())
        print(f"[{datetime.now()}] ✅ Email enviado → {recipients}")
        return True

    except smtplib.SMTPAuthenticationError:
        print(f"[{datetime.now()}] ❌ Error de autenticación SMTP")
        return False
    except smtplib.SMTPException as e:
        print(f"[{datetime.now()}] ❌ Error SMTP: {e}")
        return False
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Error inesperado al enviar email: {e}")
        return False


# ── TEST ──────────────────────────────────────────────
if __name__ == "__main__":
    resultado = send_etl_notification(
        dag_id    = "TEST_email_notifier"
      , status    = "OK"
      , contenido = "Prueba de reporte en memoria — sin leer disco"
      , extra_msg = "Prueba desde email_notifier.py"
    )
    print("✅ Test exitoso" if resultado else "❌ Test fallido")
