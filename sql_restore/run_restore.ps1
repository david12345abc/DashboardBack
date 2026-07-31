[CmdletBinding()]
param(
    [switch]$Force
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = Split-Path -Parent $scriptDir
$disableMarkers = @(
    "D:\mssql\RESTORE_DISABLED",
    (Join-Path $scriptDir "RESTORE_DISABLED")
)
foreach ($marker in $disableMarkers) {
    if (Test-Path $marker) {
        Write-Error "Restore is disabled ($marker). Remove the flag to allow restore."
        exit 2
    }
}

Set-Location $projectDir
$pyArgs = @("-m", "sql_restore.restore_native")
if ($Force) {
    # Force is exposed via Python API / Django command; CLI module uses env override.
    $env:MSSQL_RESTORE_FORCE = "1"
}

if (Get-Command py -ErrorAction SilentlyContinue) {
    & py @pyArgs
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    & python @pyArgs
} else {
    throw "Python launcher (py or python) was not found in PATH."
}

exit $LASTEXITCODE
