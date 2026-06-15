# Comandos Docker — Referencia y Diagnóstico
**Proyecto:** flybackDW-etl-airflow  
**Fecha:** 2026-06-15  
**Autor:** Andrés — Gusacapital

---

## 📋 Índice
1. [Gestión de contenedores](#gestión-de-contenedores)
2. [Verificación de variables de entorno](#verificación-de-variables-de-entorno)
3. [Ejecución de scripts Python](#ejecución-de-scripts-python)
4. [Logs y diagnóstico](#logs-y-diagnóstico)
5. [Git — flujo de trabajo](#git--flujo-de-trabajo)

---

## 🐳 Gestión de contenedores

### Arrancar todos los servicios
```powershell
cd "C:\Users\GUSA CAPITAL\Documents\DockersETL"
docker-compose up -d
```

### Detener todos los servicios
```powershell
docker-compose down
```

### Reiniciar solo el scheduler (para refrescar DAGs)
```powershell
docker-compose restart airflow-scheduler
```
> ⚠️ En PowerShell NO usar `&&` — separar en dos líneas.

### Ver contenedores corriendo
```powershell
docker ps
```

---

## 🔑 Verificación de variables de entorno

### ❌ Problema detectado — variables vacías
Las variables del `.env` NO llegan al contenedor si no se declaran
explícitamente en la sección `environment` del `docker-compose.yml`.

```powershell
# Verificar si las variables llegan al contenedor
docker exec -it airflow_scheduler bash -c "echo EMAIL_USER=$EMAIL_USER && echo EMAIL_PASSWORD=$EMAIL_PASSWORD"
```

**Resultado cuando falla:**
```
EMAIL_USER=
EMAIL_PASSWORD=
```

**Resultado cuando funciona:**
```
EMAIL_USER=andres@gusacapital.com
EMAIL_PASSWORD=Sop_C4p1t41_2023*
```

### ✅ Solución — agregar al docker-compose.yml
```yaml
airflow-scheduler:
  environment:
    - EMAIL_USER=${EMAIL_USER}
    - EMAIL_PASSWORD=${EMAIL_PASSWORD}
```

### Verificar variables con script Python
```powershell
# Crear test_env.py en dags/etl_flyback/
docker exec -it airflow_scheduler bash -c "python /opt/airflow/dags/etl_flyback/test_env.py"
```

**Contenido de `test_env.py`:**
```python
import os
print("EMAIL_USER    :", os.getenv("EMAIL_USER",     "NO ENCONTRADO"))
print("EMAIL_PASSWORD:", os.getenv("EMAIL_PASSWORD", "NO ENCONTRADO"))
```

---

## 🐍 Ejecución de scripts Python

### Ejecutar script directamente en el contenedor
```powershell
docker exec -it airflow_scheduler bash -c "python /opt/airflow/dags/etl_flyback/test_email.py"
```

### Ejecutar test de email
```powershell
docker exec -it airflow_scheduler bash -c "python /opt/airflow/dags/etl_flyback/test_email.py"
```

**Resultado exitoso:**
```
[2026-06-15 21:03:47] ✅ Email enviado → ['andres@gusacapital.com']
✅ Test exitoso — revisa tu bandeja de entrada
```

**Resultado fallido — error de autenticación:**
```
[2026-06-15 21:03:47] ❌ Error de autenticación SMTP
❌ Test fallido — revisa la configuración SMTP
```
> 💡 Si falla con error de autenticación — verificar que las variables
> de entorno lleguen al contenedor y que el puerto sea **587**.

---

## 📧 Configuración SMTP — Gusacapital

| Parámetro | Valor |
|---|---|
| Host | `mail.gusacapital.com` |
| Puerto | **587** ← importante, no 25 |
| Autenticación | ✅ Sí |
| TLS/SSL | ❌ Never |
| Usuario | `andres@gusacapital.com` |
| Password | Variable `EMAIL_PASSWORD` en `.env` |

> ⚠️ **Error común:** usar puerto 25 en lugar de 587 causa
> `SMTPAuthenticationError` aunque las credenciales sean correctas.

---

## 📋 Logs y diagnóstico

### Ver logs de un task específico
```powershell
# Buscar archivos de log de un DAG
docker exec -it airflow_scheduler bash -c "find /opt/airflow/logs/dag_id=dag_tbInicioSolAutPag_diario -name '*.log' | head -10"
```

### Leer log de un task específico
```powershell
docker exec -it airflow_scheduler bash -c "cat '/opt/airflow/logs/dag_id=dag_tbInicioSolAutPag_diario/run_id=manual__2026-06-15T20:20:56.659719+00:00/task_id=generar_log_y_notificar/attempt=1.log' | tail -30"
```
> 💡 El `run_id` contiene `:` — usar comillas simples alrededor de la ruta.

---

## 🔀 Git — flujo de trabajo

### Estado del repositorio
```powershell
git status
```

### Agregar y commitear
```powershell
git add .
git commit -m "feat: descripción del cambio"
git push
```

### Cuando el push es rechazado (remote tiene cambios)
```powershell
git stash                    # guardar cambios locales
git pull origin main         # traer cambios remotos
git stash pop                # recuperar cambios locales
git push                     # push final
```

### Resolver conflicto de merge
```powershell
git checkout --theirs README.md   # tomar versión remota
git add README.md
git commit --no-edit
git push
```

### Completar merge interrumpido
```powershell
git commit --no-edit
```

> ⚠️ **VIM** — si Git abre el editor VIM para el mensaje de merge:
> Presionar `Escape` luego escribir `:wq` y `Enter` para guardar y salir.
> ⚠️ Es sensible a mayúsculas — `:wq` funciona, `:WQ` no.

---

## 📁 Estructura del proyecto

```
DockersETL/
├── .env                    ← variables de entorno (NO commitear passwords)
├── docker-compose.yml      ← configuración de contenedores
├── dags/
│   ├── common/
│   │   ├── email_notifier.py   ← módulo de email
│   │   ├── audit_logger.py     ← módulo de logs
│   │   └── db_connections.py   ← conexiones BD
│   ├── etl/                ← DAGs de customers (global)
│   └── etl_flyback/        ← DAGs de flybackDW (reportería)
│       ├── dag_tbInicioSolAutPag_diario.py
│       ├── test_email.py   ← test de correo
│       └── test_env.py     ← test de variables
└── logs/                   ← logs generados por los DAGs
```
