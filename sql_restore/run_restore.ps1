[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$restoreScript = Join-Path $scriptDir "restore_latest_bak.py"
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

if (Get-Command py -ErrorAction SilentlyContinue) {
    & py $restoreScript
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    & python $restoreScript
} else {
    throw "Python launcher (py or python) was not found in PATH."
}

exit $LASTEXITCODE
