# ══════════════════════════════════════════════
# test_email.py
# Objetivo: Probar envío de correo via email_notifier
# Ejecutar: python test_email.py
# ══════════════════════════════════════════════
import sys
sys.path.insert(0, '/opt/airflow/dags')
from common.email_notifier import send_etl_notification

resultado = send_etl_notification(
    dag_id    = "TEST_email_notifier",
    status    = "OK",
    extra_msg = "¡Logrado! El correo electrónico funciona correctamente desde Airflow. 🎉",
)

if resultado:
    print("✅ Test exitoso — revisa tu bandeja de entrada")
else:
    print("❌ Test fallido — revisa la configuración SMTP")
