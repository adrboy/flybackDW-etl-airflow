# ============================================
# backup_etl.ps1
# Backup de DockersETL hacia USB
# Uso: .\backup_etl.ps1
# ============================================

$origen  = "C:\Users\GUSA CAPITAL\Documents\DockersETL"
$destino = "F:\DockersETL_BACKUP_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
$logFile = "F:\backup_log_$(Get-Date -Format 'yyyyMMdd_HHmmss').txt"

function Write-Log {
    param($mensaje)
    $linea = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $mensaje"
    Write-Host $linea
    Add-Content -Path $logFile -Value $linea
}

Write-Log "======================================"
Write-Log "INICIANDO BACKUP DockersETL"
Write-Log "Origen:  $origen"
Write-Log "Destino: $destino"
Write-Log "======================================"

# PASO 1 — Copiar archivos con robocopy
Write-Log "PASO 1: Copiando archivos..."

robocopy $origen $destino /E /XD "__pycache__" ".git" "postgres_data" "scheduler" /XF "*.pyc" "latest"

Write-Log "PASO 2: Verificando integridad con hash MD5..."

# PASO 2 — Verificar hash de archivos clave
$archivos = @(
    "dags\common\etl_base.py",
    "dags\common\etl_basephone.py",
    "dags\common\audit_logger.py",
    "dags\common\db_connections.py",
    "dags\etl\dag_masterclients.py",
    "dags\etl\dag_masterphones.py",
    "dags\etl\dag_master_gold.py",
    "docker-compose.yml",
    "README.md"
)

$errores = 0
foreach ($archivo in $archivos) {
    $archivoOrigen  = Join-Path $origen $archivo
    $archivoDestino = Join-Path $destino $archivo

    if (Test-Path $archivoOrigen) {
        $hashOrigen  = (Get-FileHash $archivoOrigen  -Algorithm MD5).Hash
        $hashDestino = (Get-FileHash $archivoDestino -Algorithm MD5).Hash

        if ($hashOrigen -eq $hashDestino) {
            Write-Log "✅ OK     $archivo"
        } else {
            Write-Log "❌ ERROR  $archivo - Hash diferente!"
            $errores++
        }
    } else {
        Write-Log "⚠️  SKIP   $archivo - No existe en origen"
    }
}

Write-Log "======================================"
if ($errores -eq 0) {
    Write-Log "BACKUP COMPLETADO EXITOSAMENTE"
    Write-Log "Destino: $destino"
} else {
    Write-Log "BACKUP COMPLETADO CON $errores ERRORES"
}
Write-Log "======================================"
