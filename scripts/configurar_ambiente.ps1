$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$envPath = Join-Path $projectRoot '.env'

if (Test-Path -LiteralPath $envPath) {
    Write-Host 'O arquivo .env ja existe e foi preservado.'
    exit 0
}

function New-RandomBytes([int]$Length) {
    $bytes = New-Object byte[] $Length
    $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $generator.GetBytes($bytes)
    } finally {
        $generator.Dispose()
    }
    return ,$bytes
}

function New-RandomSecret {
    return 'Aa1_' + ([BitConverter]::ToString((New-RandomBytes 32))).Replace('-', '').ToLowerInvariant()
}

$values = @{
    POSTGRES_PASSWORD = New-RandomSecret
    MSSQL_SA_PASSWORD = New-RandomSecret
    AIRFLOW_FERNET_KEY = [Convert]::ToBase64String((New-RandomBytes 32)).Replace('+', '-').Replace('/', '_')
    AIRFLOW_API_SECRET_KEY = New-RandomSecret
    AIRFLOW_JWT_SECRET = New-RandomSecret
    _AIRFLOW_WWW_USER_PASSWORD = New-RandomSecret
}

$template = [IO.File]::ReadAllText((Join-Path $projectRoot '.env.example'))
foreach ($key in $values.Keys) {
    $template = [regex]::Replace($template, '(?m)^' + [regex]::Escape($key) + '=.*$', $key + '=' + $values[$key])
}

$encoding = New-Object System.Text.UTF8Encoding $false
$stream = New-Object System.IO.FileStream($envPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write)
try {
    $bytes = $encoding.GetBytes($template)
    $stream.Write($bytes, 0, $bytes.Length)
} finally {
    $stream.Dispose()
}

Write-Host 'Arquivo .env criado com credenciais aleatorias exclusivas. Os valores permanecem somente no arquivo local.'
