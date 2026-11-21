$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$destinationDirectory = Join-Path $projectRoot 'output/entrega'
$destination = Join-Path $destinationDirectory 'ApacheAirflow-e-ETL.zip'
$temporary = Join-Path $destinationDirectory 'ApacheAirflow-e-ETL.zip.tmp'
$entries = [System.Collections.Generic.List[object]]::new()

foreach ($name in @('.env.example', '.gitignore', 'docker-compose.yml', 'readme.md', 'scripts/configurar_ambiente.ps1', 'scripts/gerar_entrega.ps1')) {
    $path = Join-Path $projectRoot $name
    if (!(Test-Path -LiteralPath $path -PathType Leaf)) {
        throw "Arquivo obrigatorio ausente: $name"
    }
    $entries.Add([pscustomobject]@{ Path = $path; Name = $name })
}

foreach ($directory in @('dags', 'data')) {
    foreach ($file in Get-ChildItem -LiteralPath (Join-Path $projectRoot $directory) -File -Recurse) {
        if ($file.FullName -match '[\\/]__pycache__[\\/]') { continue }
        $extensions = if ($directory -eq 'dags') { @('.py', '.sql') } else { @('.png', '.drawio') }
        if ($file.Extension -notin $extensions) { continue }
        $relative = $file.FullName.Substring($projectRoot.Length + 1).Replace('\', '/')
        $entries.Add([pscustomobject]@{ Path = $file.FullName; Name = $relative })
    }
}

$secretValues = [System.Collections.Generic.List[string]]::new()
$envPath = Join-Path $projectRoot '.env'
if (Test-Path -LiteralPath $envPath) {
    foreach ($line in [IO.File]::ReadAllLines($envPath)) {
        if ($line -match '^([^=]*(?:PASSWORD|SECRET|FERNET)[^=]*)=(.+)$') {
            $value = $Matches[2].Trim().Trim([char]34, [char]39)
            if ($value.Length -ge 8) { $secretValues.Add($value) }
        }
    }
}

foreach ($entry in $entries) {
    $content = [Text.Encoding]::UTF8.GetString([IO.File]::ReadAllBytes($entry.Path))
    foreach ($value in $secretValues) {
        if ($content.Contains($value)) {
            throw "Credencial local encontrada em arquivo de entrega: $($entry.Name)"
        }
    }
}

[IO.Directory]::CreateDirectory($destinationDirectory) | Out-Null
Add-Type -AssemblyName System.IO.Compression
Add-Type -AssemblyName System.IO.Compression.FileSystem
$stream = [IO.File]::Open($temporary, [IO.FileMode]::Create)
$archive = [IO.Compression.ZipArchive]::new($stream, [IO.Compression.ZipArchiveMode]::Create)
try {
    foreach ($entry in $entries) {
        [IO.Compression.ZipFileExtensions]::CreateEntryFromFile($archive, $entry.Path, 'ApacheAirflow-e-ETL/' + $entry.Name, [IO.Compression.CompressionLevel]::Optimal) | Out-Null
    }
    foreach ($directory in @('config/', 'plugins/', 'logs/')) {
        $archive.CreateEntry('ApacheAirflow-e-ETL/' + $directory) | Out-Null
    }
} finally {
    $archive.Dispose()
    $stream.Dispose()
}

[IO.File]::Copy($temporary, $destination, $true)
[IO.File]::Delete($temporary)
Write-Host "Entrega criada: $destination"
