[CmdletBinding()]
param([switch]$Build)
$ErrorActionPreference = 'Stop'
Push-Location -LiteralPath $PSScriptRoot
try {
    docker version --format '{{.Server.Version}}'
    if ($LASTEXITCODE -ne 0) { throw 'Open Docker Desktop and wait for Engine running.' }
    if (-not (Test-Path -LiteralPath '.env')) {
        $secretBytes = New-Object byte[] 32
        $generator = [System.Security.Cryptography.RandomNumberGenerator]::Create()
        try { $generator.GetBytes($secretBytes) } finally { $generator.Dispose() }
        $secretText = -join ($secretBytes | ForEach-Object { $_.ToString('x2') })
        [System.IO.File]::WriteAllText((Join-Path $PSScriptRoot '.env'),
            "LAB3_ACCESS_KEY=lab3processor`nLAB3_SECRET_KEY=$secretText`n",
            [System.Text.UTF8Encoding]::new($false))
    }
    docker compose config --quiet
    if ($LASTEXITCODE -ne 0) { throw 'Invalid Compose configuration.' }
    if ($Build) {
        docker compose build driver objects
        if ($LASTEXITCODE -ne 0) { throw 'Image build failed. Resolve the reported dependency error before continuing.' }
    }
    docker image inspect lab3-spark-runtime:4.0.1 lab3-minio:RELEASE.2025-04-22T22-12-26Z --format '{{json .RepoTags}} {{.Id}}'
    if ($LASTEXITCODE -ne 0) { throw 'Required images are missing. Run Setup-Lab3.ps1 -Build.' }
    Write-Host 'Runtime images and private environment configuration are ready.'
} finally { Pop-Location }
