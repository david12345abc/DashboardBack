[CmdletBinding()]
param(
    [string]$TaskName = "Dashboard - Daily 1C SQL Restore",
    [string]$DailyAt = "00:00",
    # First scheduled run: Tuesday 2026-07-28 00:00 (local time).
    [string]$FirstRunDate = "2026-07-28"
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = Split-Path -Parent $scriptDir
$envPath = Join-Path $projectDir ".env"
$runnerPath = Join-Path $scriptDir "run_restore.ps1"

if (-not (Test-Path -LiteralPath $envPath)) {
    throw ".env not found: $envPath"
}

$settings = @{}
Get-Content -LiteralPath $envPath -Encoding UTF8 | ForEach-Object {
    $line = $_.Trim()
    if (-not $line -or $line.StartsWith("#") -or -not $line.Contains("=")) {
        return
    }
    $key, $value = $line.Split("=", 2)
    $settings[$key.Trim()] = $value.Trim().Trim('"').Trim("'")
}

$runAsUser = $settings["LOGIN"]
$runAsPassword = $settings["PASSWORD"]
if (-not $runAsUser -or -not $runAsPassword) {
    throw "LOGIN and PASSWORD must be set in $envPath"
}

try {
    $startAt = [DateTime]::ParseExact(
        "$FirstRunDate $DailyAt",
        "yyyy-MM-dd HH:mm",
        [Globalization.CultureInfo]::InvariantCulture
    )
} catch {
    throw "FirstRunDate must be yyyy-MM-dd and DailyAt HH:mm (e.g. 2026-07-28 / 00:00)."
}

$actionArguments = (
    '-NoProfile -NonInteractive -ExecutionPolicy Bypass -File "{0}"' -f $runnerPath
)
$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument $actionArguments `
    -WorkingDirectory $projectDir
# Daily trigger whose StartBoundary is the first intended run (not "today").
$trigger = New-ScheduledTaskTrigger -Daily -At $startAt
# Large 1C restores can exceed a day; keep the task alive for the poll deadline.
# Restarts are safe: the Python script polls an in-progress RESTORE and never
# re-issues WITH REPLACE while the database is RESTORING.
$taskSettings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 72) `
    -RestartCount 1 `
    -RestartInterval (New-TimeSpan -Minutes 30)

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger $trigger `
    -Settings $taskSettings `
    -User $runAsUser `
    -Password $runAsPassword `
    -RunLevel Limited `
    -Description "Restore the latest 1C SQL .bak from SMB; data on D:, transaction logs on C:." `
    -Force | Out-Null

$task = Get-ScheduledTask -TaskName $TaskName
$info = Get-ScheduledTaskInfo -TaskName $TaskName
Write-Host "Scheduled task registered:"
Write-Host "  Name: $($task.TaskName)"
Write-Host "  State: $($task.State)"
Write-Host "  Daily at: $DailyAt"
Write-Host "  FirstRunDate: $FirstRunDate"
Write-Host "  StartBoundary: $($trigger.StartBoundary)"
Write-Host "  NextRunTime: $($info.NextRunTime)"
