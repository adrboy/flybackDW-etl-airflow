# ============================================
# backup_etl_v2.ps1
# Backup COMPLETO de DockersETL hacia USB
# Incluye postgres_data para restauracion completa
# Con barra de progreso
# Uso: .\backup_etl_v2.ps1
# ============================================

$origen  = "C:\Users\GUSA CAPITAL\Documents\DockersETL"
$destino = "F:\DockersETL_BACKUP_FULL_$(Get-Date -Format 'yyyyMMdd_HHmmss')"
$logFile = "F:\backup_log_FULL_$(Get-Date -Format 'yyyyMMdd_HHmmss').txt"

function Write-Log {
    param($mensaje)
    $linea = "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')] $mensaje"
    Write-Host $linea
    Add-Content -Path $logFile -Value $linea
}

Write-Log "======================================"
Write-Log "INICIANDO BACKUP COMPLETO DockersETL"
Write-Log "Incluye: postgres_data (restauracion completa)"
Write-Log "Origen:  $origen"
Write-Log "Destino: $destino"
Write-Log "======================================"

# PASO 1 - Contar archivos para la barra de progreso
Write-Log "PASO 1: Calculando archivos a copiar..."

$archivosACopiar = Get-ChildItem $origen -Recurse -File | 
    Where-Object { 
        $_.FullName -notmatch "__pycache__" -and 
        $_.FullName -notmatch "\\.git\\" -and
        $_.FullName -notmatch "scheduler" -and
        $_.Name -ne "latest" -and
        $_.Extension -ne ".pyc"
    }

$totalArchivos = $archivosACopiar.Count
Write-Log "Total archivos a copiar: $totalArchivos"

# PASO 2 - Copiar archivos con barra de progreso
Write-Log "PASO 2: Copiando archivos..."

$copiados = 0

foreach ($archivo in $archivosACopiar) {
    $rutaRelativa   = $archivo.FullName.Substring($origen.Length)
    $rutaDestino    = Join-Path $destino $rutaRelativa
    $carpetaDestino = Split-Path $rutaDestino -Parent

    if (-not (Test-Path $carpetaDestino)) {
        New-Item -ItemType Directory -Path $carpetaDestino -Force | Out-Null
    }

    try {
        Copy-Item -Path $archivo.FullName -Destination $rutaDestino -Force
        $copiados++
    } catch {
        Write-Log "SKIP: $($archivo.Name)"
    }

    $porcentaje = [math]::Round(($copiados / $totalArchivos) * 100)
    Write-Progress -Activity "Copiando DockersETL..." `
                   -Status "$copiados de $totalArchivos archivos ($porcentaje%)" `
                   -PercentComplete $porcentaje `
                   -CurrentOperation $archivo.Name
}

Write-Progress -Activity "Copiando DockersETL..." -Completed
Write-Log "Archivos copiados: $copiados de $totalArchivos"

# PASO 3 - Verificar hash de archivos clave
Write-Log "PASO 3: Verificando integridad con hash MD5..."

$archivosHash = @(
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

$errores     = 0
$totalHash   = $archivosHash.Count
$contadorHash = 0

foreach ($archivo in $archivosHash) {
    $contadorHash++
    $porcentaje = [math]::Round(($contadorHash / $totalHash) * 100)

    Write-Progress -Activity "Verificando hash MD5..." `
                   -Status "$contadorHash de $totalHash archivos ($porcentaje%)" `
                   -PercentComplete $porcentaje `
                   -CurrentOperation $archivo

    $archivoOrigen  = Join-Path $origen $archivo
    $archivoDestino = Join-Path $destino $archivo

    if (Test-Path $archivoOrigen) {
        $hashOrigen  = (Get-FileHash $archivoOrigen  -Algorithm MD5).Hash
        $hashDestino = (Get-FileHash $archivoDestino -Algorithm MD5).Hash

        if ($hashOrigen -eq $hashDestino) {
            Write-Log "OK     $archivo"
        } else {
            Write-Log "ERROR  $archivo - Hash diferente!"
            $errores++
        }
    } else {
        Write-Log "SKIP   $archivo - No existe en origen"
    }
}

Write-Progress -Activity "Verificando hash MD5..." -Completed

# PASO 4 - Verificar postgres_data
Write-Log "PASO 4: Verificando postgres_data..."
$pgOrigen  = Join-Path $origen "postgres_data"
$pgDestino = Join-Path $destino "postgres_data"

if (Test-Path $pgDestino) {
    $countOrigen  = (Get-ChildItem $pgOrigen  -Recurse -File).Count
    $countDestino = (Get-ChildItem $pgDestino -Recurse -File).Count
    Write-Log "postgres_data origen:  $countOrigen archivos"
    Write-Log "postgres_data destino: $countDestino archivos"
    if ($countOrigen -eq $countDestino) {
        Write-Log "OK     postgres_data"
    } else {
        Write-Log "AVISO  postgres_data - diferencia de archivos"
    }
} else {
    Write-Log "ERROR  postgres_data no se copio"
    $errores++
}

Write-Log "======================================"
Write-Log "INSTRUCCIONES DE RESTAURACION:"
Write-Log "1. Copiar esta carpeta a la PC destino"
Write-Log "2. Instalar Docker Desktop"
Write-Log "3. docker-compose up -d"
Write-Log "4. Las conexiones y historial se restauran solos"
Write-Log "======================================"

if ($errores -eq 0) {
    Write-Log "BACKUP COMPLETO EXITOSAMENTE"
    Write-Log "Destino: $destino"
} else {
    Write-Log "BACKUP COMPLETADO CON $errores ADVERTENCIAS"
}
Write-Log "======================================"
