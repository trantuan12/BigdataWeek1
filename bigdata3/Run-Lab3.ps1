[CmdletBinding()]
param(
    [ValidateSet('all','start','preflight','bootstrap','incremental','analytics','benchmark','release','verify','package','stop')]
    [string]$Step = 'all'
)
$ErrorActionPreference = 'Stop'
function Invoke-Docker {
    param([string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker command failed ($LASTEXITCODE). Inspect its reported error." }
}
Push-Location -LiteralPath $PSScriptRoot
try {
    if ($Step -eq 'stop') { Invoke-Docker -Arguments @('compose','stop'); return }
    $configuration = Get-Content -LiteralPath 'config/team.json' -Raw -Encoding UTF8 | ConvertFrom-Json
    $evidenceDirectory = Join-Path $PSScriptRoot $configuration.evidence_dir
    if ($Step -eq 'all' -and (Test-Path -LiteralPath (Join-Path $configuration.evidence_dir 'state.json'))) {
        throw 'Snapshots already exist. Resume a stage, or set a new run_label AND evidence_dir for a fresh run.'
    }
    if ($Step -in @('all','start')) {
        Invoke-Docker -Arguments @('compose','up','-d','--no-build')
        Invoke-Docker -Arguments @('compose','exec','-T','driver','python','infrastructure/prepare_storage.py')
        $imageEvidence = & docker image inspect lab3-spark-runtime:4.0.1 lab3-minio:RELEASE.2025-04-22T22-12-26Z --format '{{json .RepoTags}} {{json .Id}} {{json .RepoDigests}}'
        if ($LASTEXITCODE -ne 0) { throw 'Image identity check failed.' }
        [System.IO.File]::WriteAllLines((Join-Path $evidenceDirectory 'container_images.txt'),
            [string[]]$imageEvidence,[System.Text.UTF8Encoding]::new($false))
        if ($Step -eq 'start') { return }
    }
    $stages = if ($Step -eq 'all') { @('preflight','bootstrap','incremental','analytics','benchmark','release','verify','package') } else { @($Step) }
    foreach ($stage in $stages) {
        Write-Host "Running $stage ..."
        if ($stage -eq 'analytics') {
            Invoke-Docker -Arguments @('compose','exec','-T','driver','python','tools/analytics_and_dashboard.py','--config','config/team.json')
        } elseif ($stage -eq 'verify') {
            Invoke-Docker -Arguments @('compose','exec','-T','driver','python','tools/verify_results.py','--config','config/team.json','--trusted-manifest','/instructor/trusted_lab3_inputs.json')
            Invoke-Docker -Arguments @('compose','exec','-T','driver','python','tools/verify_live_tables.py','--config','config/team.json')
            Invoke-Docker -Arguments @('compose','exec','-T','driver','python','tests/run_tests.py')
            Invoke-Docker -Arguments @('compose','exec','-T','driver','python','tools/capture_benchmark_metrics.py','--config','config/team.json')
            Invoke-Docker -Arguments @('compose','exec','-T','driver','python','tools/check_role_d.py','--config','config/team.json')
            Invoke-Docker -Arguments @('compose','exec','-T','driver','python','tools/check_role_a.py','--config','config/team.json','--trusted-manifest','/instructor/trusted_lab3_inputs.json')
            Invoke-Docker -Arguments @('compose','exec','-T','driver','python','tools/check_role_e.py','--config','config/team.json','--trusted-manifest','/instructor/trusted_lab3_inputs.json')
        } elseif ($stage -eq 'package') {
            Invoke-Docker -Arguments @('compose','exec','-T','driver','python','tools/portable_delta.py','export','--reuse-existing')
            Invoke-Docker -Arguments @('compose','exec','-T','driver','python','tools/portable_delta.py','verify')
            Invoke-Docker -Arguments @('compose','exec','-T','driver','python','tools/prepare_role_d_submission.py')
        } else {
            $stageArguments = @('compose','exec','-T','driver','python','src/run_lab.py','--config','config/team.json','--trusted-manifest','/instructor/trusted_lab3_inputs.json','--step',$stage)
            if ($stage -eq 'release') {
                $taskMembers = Get-Content -LiteralPath 'team.json' -Raw -Encoding UTF8 | ConvertFrom-Json
                $transformationMember = $taskMembers | Where-Object { $_.role -eq 'D' }
                $reviewMember = $taskMembers | Where-Object { $_.role -eq 'E' }
                $stewardRole = if ($configuration.steward_role) { $configuration.steward_role } else { 'E' }
                $stewardMember = $taskMembers | Where-Object { $_.role -eq $stewardRole }
                foreach ($taskMember in @($transformationMember,$reviewMember,$stewardMember)) {
                    if (-not $taskMember.student_id -or -not $taskMember.name) { throw 'Missing author/reviewer/steward assignment in team.json.' }
                }
                $stageArguments += @('--author',($transformationMember.student_id+' - '+$transformationMember.name),
                    '--reviewer',($reviewMember.student_id+' - '+$reviewMember.name),
                    '--steward',($stewardMember.student_id+' - '+$stewardMember.name))
            }
            Invoke-Docker -Arguments $stageArguments
        }
    }
} finally { Pop-Location }
