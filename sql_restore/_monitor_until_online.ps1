# Monitor restore until ONLINE + state.json, with container-death recovery.
$ErrorActionPreference = "Continue"
$projectDir = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $projectDir
$log = "D:\mssql\logs\restore_monitor_eta.log"
$deadline = (Get-Date).AddHours(48)
"monitor_v2 start $(Get-Date -Format o)" | Add-Content $log

function Get-ValidBytes([string]$path) {
    if (-not (Test-Path -LiteralPath $path)) { return [int64]0 }
    $out = fsutil file queryvaliddata $path 2>&1 | Out-String
    if ($out -match '\((\d+)\)') { return [int64]$Matches[1] }
    return [int64]0
}

function Test-RestorePython {
    return [bool](Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
        Where-Object { $_.CommandLine -match 'restore_latest_bak' })
}

function Get-ContainerStatus {
    docker ps -a --filter name=dashboard-erp-mssql --format "{{.Status}}" 2>$null
}

function Ensure-Container {
    $status = Get-ContainerStatus
    if ($status -match '^Up') { return $true }
    Write-Host "CONTAINER_DOWN status=$status - starting"
    docker compose --project-directory sql_restore --env-file .env -f sql_restore/docker-compose.yml up -d | Out-Host
    Start-Sleep 25
    return ((Get-ContainerStatus) -match '^Up')
}

function Start-RestoreIfNeeded {
    if (Test-RestorePython) { return }
    $mdf = "D:\mssql\data\erp_pm_data_1.mdf"
    $out = "D:\mssql\logs\restore_manual_autorecover.log"
    $err = "D:\mssql\logs\restore_manual_autorecover.err.log"
    "AUTORECOVER $(Get-Date -Format o) mdfExists=$(Test-Path $mdf)" | Add-Content $out
    Write-Host "STARTING_RESTORE_PYTHON"
    Start-Process -FilePath py -ArgumentList "sql_restore\restore_latest_bak.py" `
        -WorkingDirectory $projectDir -RedirectStandardOutput $out `
        -RedirectStandardError $err -WindowStyle Hidden | Out-Null
}

while ((Get-Date) -lt $deadline) {
    $containerOk = Ensure-Container
    $py = Test-RestorePython
    $mdfPath = "D:\mssql\data\erp_pm_data_1.mdf"
    $ldfPath = "C:\mssql\log\erp_pm_log_1.ldf"
    $mdfExists = Test-Path $mdfPath
    $ldfExists = Test-Path $ldfPath

    if (-not $containerOk) {
        $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') BLOCKER_CONTAINER_WONT_START"
        Add-Content $log $line; Write-Host $line
        break
    }

    # If container was killed and DB files wiped, restart restore script.
    if (-not $py -and -not $mdfExists) {
        Start-RestoreIfNeeded
        Start-Sleep 30
        $py = Test-RestorePython
    }
    elseif (-not $py -and $mdfExists) {
        # Files exist; script may have died while SQL still restoring — restart poller only.
        Start-RestoreIfNeeded
        $py = Test-RestorePython
    }

    $st = ""
    try { $st = (py sql_restore\_status_once.py 2>&1 | Out-String).Trim() } catch { $st = "status_err=$_" }
    $stateLine = (($st -split "`n") | Where-Object { $_ -match '^state=' } | Select-Object -First 1)
    $progLine = (($st -split "`n") | Where-Object { $_ -match '^progress' } | Select-Object -First 1)
    $vmdf = Get-ValidBytes $mdfPath
    $vldf = Get-ValidBytes $ldfPath
    $mdfGB = if ($mdfExists) { [math]::Round((Get-Item $mdfPath).Length / 1GB, 2) } else { 0 }
    $ldfGB = if ($ldfExists) { [math]::Round((Get-Item $ldfPath).Length / 1GB, 2) } else { 0 }
    $cFree = [math]::Round((Get-PSDrive C).Free / 1GB, 2)
    $dFree = [math]::Round((Get-PSDrive D).Free / 1GB, 2)
    $line = "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') py=$py $stateLine $progLine mdfGB=$mdfGB validMdfGB=$([math]::Round($vmdf/1GB,2)) ldfGB=$ldfGB validLdfGB=$([math]::Round($vldf/1GB,2)) Cfree=$cFree Dfree=$dFree"
    Add-Content $log $line
    Write-Host $line

    if ($st -match 'state=ONLINE' -and $st -match 'progress=none') {
        Write-Host "DB_ONLINE_DETECTED"
        for ($i = 0; $i -lt 90; $i++) {
            if (Test-Path D:\mssql\state.json) {
                Write-Host "STATE_JSON_WRITTEN"
                Get-Content D:\mssql\state.json
                break
            }
            Start-Sleep 10
        }
        break
    }

    if ($cFree -lt 3 -or $dFree -lt 3) {
        Write-Host "BLOCKER_DISK C=$cFree D=$dFree"
        break
    }

    Start-Sleep -Seconds 180
}

Write-Host "MONITOR_END $(Get-Date -Format o)"
if (Test-Path D:\mssql\state.json) {
    Write-Host "FINAL_STATE_JSON"
    Get-Content D:\mssql\state.json
} else {
    Write-Host "FINAL_STATE_JSON=missing"
}
py sql_restore\_status_once.py
Get-ScheduledTaskInfo -TaskName "Dashboard - Daily 1C SQL Restore" | Format-List NextRunTime
