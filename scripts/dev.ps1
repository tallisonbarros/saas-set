param(
  [string]$HostAddress = "127.0.0.1",
  [int]$Port = 8000,
  [switch]$SkipMigrate,
  [switch]$Check
)

$ErrorActionPreference = "Stop"

$ProjectRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$Python = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$ManagePy = Join-Path $ProjectRoot "manage.py"

if (-not (Test-Path $Python)) {
  Write-Error "Virtualenv nao encontrado. Crie com: python -m venv .venv"
}

if (-not (Test-Path $ManagePy)) {
  Write-Error "manage.py nao encontrado em $ProjectRoot"
}

if ($Check) {
  Write-Host "Ambiente pronto para rodar o servidor Django."
  Write-Host "Python: $Python"
  if ($SkipMigrate) {
    Write-Host "Migracoes: ignoradas por -SkipMigrate"
  } else {
    Write-Host "Migracoes: serao aplicadas antes do servidor"
  }
  Write-Host "Servidor: http://$HostAddress`:$Port/"
  exit 0
}

Set-Location $ProjectRoot
if (-not $SkipMigrate) {
  Write-Host "Aplicando migracoes..."
  & $Python $ManagePy migrate
}

& $Python $ManagePy runserver "$HostAddress`:$Port"
