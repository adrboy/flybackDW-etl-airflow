# dags/common/email_notifier.py
# ══════════════════════════════════════════════
# Módulo: email_notifier
# Objetivo: Enviar notificación por email con
#           el contenido de un log ETL
# Autor: Andrés — Gusacapital
# Fecha: 2026-06-03
# Update: 2026-06-15 — puerto 587 con autenticación
# ══════════════════════════════════════════════

import smtplib
import os
from email.mime.text        import MIMEText
from email.mime.multipart   import MIMEMultipart
from datetime               import datetime

# ══════════════════════════════════════════════
# Configuración del servidor de correo
# Gusacapital — puerto 587, con autenticación, sin TLS/SSL
# ══════════════════════════════════════════════

SMTP_HOST     = "mail.gusacapital.com"
SMTP_PORT     = 587                                          # ← corregido
SMTP_USER     = os.getenv("EMAIL_USER",     "andres@gusacapital.com")
SMTP_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
SMTP_FROM     = "andres@gusacapital.com"

# ══════════════════════════════════════════════
# Función principal de notificación
# ══════════════════════════════════════════════

def send_etl_notification(
    dag_id      : str,          # Nombre del DAG o proceso ETL
    status      : str,          # 'OK' o 'ERROR'
    log_path    : str  = None,  # Ruta al archivo .txt del log
    recipients  : list = None,  # Lista de destinatarios
    extra_msg   : str  = None   # Mensaje adicional opcional
):
    """
    Envía un email con el resultado de un proceso ETL.

    Parámetros:
        dag_id     : Nombre del DAG (ej. 'flybackDW_spInsertHistoricoCobranza')
        status     : 'OK' o 'ERROR'
        log_path   : Ruta al archivo de log (opcional)
        recipients : Lista de emails destino (default: andres@gusacapital.com)
        extra_msg  : Texto adicional para incluir en el body
    """

    # ── Destinatarios por defecto ──────────────
    if recipients is None:
        recipients = ["andres@gusacapital.com"]

    # ── Leer el log si se proporcionó ruta ─────
    log_content = ""
    if log_path and os.path.exists(log_path):
        with open(log_path, "r", encoding="utf-8") as f:
            log_content = f.read()
    elif log_path:
        log_content = f"[AVISO] Archivo de log no encontrado: {log_path}"

    # ── Construir asunto ───────────────────────
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
    emoji     = "✅" if status == "OK" else "❌"
    subject   = f"{emoji} ETL {dag_id} — {status} — {timestamp}"

    # ── Construir body del email ───────────────
    body = f"""
ETL Notification — Gusacapital
{'='*50}

Proceso  : {dag_id}
Estado   : {status}
Fecha    : {timestamp}

{'='*50}
"""
    if extra_msg:
        body += f"\nDetalle:\n{extra_msg}\n\n{'='*50}\n"

    if log_content:
        body += f"\nContenido del Log:\n{'-'*50}\n{log_content}"
    else:
        body += "\n[Sin archivo de log adjunto]"

    # ── Construir el mensaje MIME ──────────────
    msg             = MIMEMultipart()
    msg["From"]     = SMTP_FROM
    msg["To"]       = ", ".join(recipients)
    msg["Subject"]  = subject
    msg.attach(MIMEText(body, "plain", "utf-8"))

    # ── Enviar via SMTP puerto 587 con autenticación sin TLS ──
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.login(SMTP_USER, SMTP_PASSWORD)          # ← autenticación
            server.sendmail(
                SMTP_FROM
              , recipients
              , msg.as_string()
            )
        print(f"[{datetime.now()}] ✅ Email enviado → {recipients}")
        return True

    except smtplib.SMTPAuthenticationError:
        print(f"[{datetime.now()}] ❌ Error de autenticación SMTP")
        return False
    except smtplib.SMTPException as e:
        print(f"[{datetime.now()}] ❌ Error SMTP: {e}")
        return False
    except Exception as e:
        print(f"[{datetime.now()}] ❌ Error inesperado: {e}")
        return False


# ══════════════════════════════════════════════
# TEST — Ejecutar directamente para probar
# python email_notifier.py
# ══════════════════════════════════════════════

if __name__ == "__main__":

    log_prueba = r"C:\Users\GUSA CAPITAL\Documents\DockersETL\logs\etl_phonefb_FB_log_20260528170435.txt"

    resultado = send_etl_notification(
        dag_id    = "TEST_email_notifier"
      , status    = "OK"
      , log_path  = log_prueba
      , extra_msg = "Prueba de notificación ETL desde Gusacapital"
    )

    if resultado:
        print("✅ Test exitoso — revisa tu bandeja de entrada")
    else:
        print("❌ Test fallido — revisa la configuración SMTP")
