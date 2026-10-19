param(
    [ValidateSet('Init', 'Check', 'Web', 'Worker')]
    [string]$Mode = 'Web',
    [int]$Port = 8003,
    [string]$Snapshot = '',
    [string]$Python = ''
)
$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
if (-not $Snapshot) { $Snapshot = Join-Path $projectRoot 'data/local-demo/snapshot-v43' }
if (-not $Python) {
    foreach ($candidate in @((Join-Path $projectRoot '.venv/Scripts/python.exe'), (Join-Path (Split-Path -Parent $projectRoot) '.venv/Scripts/python.exe'))) {
        if (Test-Path -LiteralPath $candidate) { $Python = $candidate; break }
    }
}
if (-not $Python -or -not (Test-Path -LiteralPath $Python)) { throw 'Pass -Python with the installed Python executable.' }
$Python = (Resolve-Path -LiteralPath $Python).Path
$Snapshot = (Resolve-Path -LiteralPath $Snapshot).Path
$products = Join-Path $Snapshot 'products.json'
if (-not (Test-Path -LiteralPath $products)) { throw "Catalog not found: $products" }
$runtimeRoot = Join-Path $projectRoot 'outputs/local-demo-v43'
New-Item -ItemType Directory -Path $runtimeRoot -Force | Out-Null
$env:PYTHONPATH = Join-Path $projectRoot 'src'
$env:INTERIOR_RECOMMENDER_PRODUCTS = $products
$env:INTERIOR_RECOMMENDER_DEMO = '0'
$env:INTERIOR_RECOMMENDER_OUTPUT_ROOT = Join-Path $runtimeRoot 'recommendations'
$env:ROOMPICK_DB_PATH = Join-Path $runtimeRoot 'db.sqlite3'
$env:ROOMPICK_MEDIA_ROOT = Join-Path $runtimeRoot 'media'
$env:ROOMPICK_RAW_ROOT = Join-Path $runtimeRoot 'raw'
$manage = Join-Path $projectRoot 'src/demo_sketchon/manage.py'

# Every process uses the same fixed catalog, SQLite database and output root.
Write-Host "Local demo: $Mode | $products"
switch ($Mode) {
    'Init' { & $Python $manage migrate --noinput }
    'Check' {
        & $Python -m interior_recommender.catalog.local_snapshot --verify $Snapshot
        if ($LASTEXITCODE -ne 0) { throw 'Snapshot integrity check failed.' }
        & $Python $manage check
    }
    'Web' { & $Python $manage runserver "127.0.0.1:$Port" --noreload }
    'Worker' { & $Python $manage run_recommendation_worker }
}
if ($LASTEXITCODE -ne 0) { throw "Local demo $Mode exited with code $LASTEXITCODE" }
