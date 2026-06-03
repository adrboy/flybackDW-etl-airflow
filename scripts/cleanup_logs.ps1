# ============================================
# cleanup_logs.ps1
# Limpieza de logs antiguos de Airflow
# Uso: .\cleanup_logs.ps1
# ============================================

$logBase    = "C:\Users\GUSA CAPITAL\Documents\DockersETL\logs"
$diasLogs   = 30
$diasTxt    = 90

function Write-Log {
    param($mensaje)
    $linea = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $mensaje"
    Write-Host $linea
}

Write-Log "======================================"
Write-Log "LIMPIEZA DE LOGS DockersETL"
Write-Log "Scheduler: eliminar > $diasLogs dias"
Write-Log "Archivos .txt: eliminar > $diasTxt dias"
Write-Log "======================================"

# PASO 1 - Limpiar logs del scheduler de Airflow (> 30 dias)
Write-Log "PASO 1: Limpiando logs scheduler (> $diasLogs dias)..."

$carpetasViejas = Get-ChildItem "$logBase\scheduler" -Recurse -Directory |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$diasLogs) }

$totalCarpetas = $carpetasViejas.Count
Write-Log "Carpetas a eliminar: $totalCarpetas"

foreach ($carpeta in $carpetasViejas) {
    try {
        Remove-Item $carpeta.FullName -Recurse -Force
        Write-Log "ELIMINADO: $($carpeta.Name)"
    } catch {
        Write-Log "SKIP: $($carpeta.Name) - en uso"
    }
}

# PASO 2 - Limpiar archivos .txt de ETL (> 90 dias)
Write-Log "PASO 2: Limpiando archivos .txt (> $diasTxt dias)..."

$txtViejos = Get-ChildItem $logBase -Recurse -Filter "*.txt" |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$diasTxt) }

$totalTxt = $txtViejos.Count
Write-Log "Archivos .txt a eliminar: $totalTxt"

foreach ($archivo in $txtViejos) {
    try {
        Remove-Item $archivo.FullName -Force
        Write-Log "ELIMINADO: $($archivo.Name)"
    } catch {
        Write-Log "SKIP: $($archivo.Name) - en uso"
    }
}

# PASO 3 - Limpiar dag_processor_manager (> 30 dias)
Write-Log "PASO 3: Limpiando dag_processor_manager (> $diasLogs dias)..."

$dagProcessor = Get-ChildItem "$logBase\dag_processor_manager" -Recurse -File |
    Where-Object { $_.LastWriteTime -lt (Get-Date).AddDays(-$diasLogs) }

$totalProcessor = $dagProcessor.Count
Write-Log "Archivos a eliminar: $totalProcessor"

foreach ($archivo in $dagProcessor) {
    try {
        Remove-Item $archivo.FullName -Force
        Write-Log "ELIMINADO: $($archivo.Name)"
    } catch {
        Write-Log "SKIP: $($archivo.Name) - en uso"
    }
}

# RESUMEN
Write-Log "======================================"
Write-Log "LIMPIEZA COMPLETADA"
Write-Log "Scheduler eliminados:      $totalCarpetas carpetas"
Write-Log "Archivos .txt eliminados:  $totalTxt archivos"
Write-Log "dag_processor eliminados:  $totalProcessor archivos"
Write-Log "======================================"
