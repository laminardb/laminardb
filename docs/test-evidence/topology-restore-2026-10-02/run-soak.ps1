$ErrorActionPreference = 'Stop'
$env:RUST_MIN_STACK = '4194304'
$env:LAMINAR_SOAK_SECONDS = '5'
$env:LAMINAR_SOAK_KILLS = '1'
$env:LAMINAR_SOAK_KAFKA_PARTITIONS = '12'
$env:LAMINAR_SOAK_CHECKPOINT_SLO_MODE = 'observe'
$env:LAMINAR_SOAK_HOT_SLO_MODE = 'observe'
$env:LAMINAR_SOAK_CHECKPOINT_URL = 's3://topology-tests-9929/checkpoints'
$env:LAMINAR_SOAK_S3_ENDPOINT = 'http://127.0.0.1:19000'
$env:LAMINAR_SOAK_S3_ACCESS_KEY = 'laminar'
$env:LAMINAR_SOAK_S3_SECRET_KEY = 'laminar-test-secret'
$env:LAMINAR_SOAK_S3_REGION = 'us-east-1'
$env:LAMINAR_SOAK_KAFKA_SOURCE_BROKERS = '127.0.0.1:19092'
$env:LAMINAR_SOAK_LAMINARDB_EXE = (Resolve-Path -LiteralPath target/topology-evidence/laminardb-restore-test-stack.exe).Path
$env:LAMINAR_SOAK_LAMINARDB_SHA256 = (Get-FileHash -LiteralPath $env:LAMINAR_SOAK_LAMINARDB_EXE -Algorithm SHA256).Hash.ToLowerInvariant()
$cutHarnessPath = (Resolve-Path -LiteralPath target/soak/deps/cluster_soak-11a5f790e393356a.exe).Path
$cutOutputDirectory = (Resolve-Path -LiteralPath target/topology-evidence).Path
$cutRunStarted = [DateTime]::UtcNow
$cutProcesses = @{}
$cutCombinedWorkingSet = 0L
$cutSampleCount = 0L
$cutHarnessPeakWorkingSet = 0L
$cutHarnessPeakPrivateBytes = 0L
$cutRunner = Start-Process -FilePath $cutHarnessPath -WorkingDirectory (Get-Location).Path -ArgumentList @('three_node_alo_topology_cut_abort_restart_soak', '--exact', '--ignored', '--nocapture') -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $cutOutputDirectory 'restore-soak-01.stdout.txt') -RedirectStandardError (Join-Path $cutOutputDirectory 'restore-soak-01.stderr.txt')
while (-not $cutRunner.HasExited) {
    $cutRunner.Refresh()
    $cutHarnessPeakWorkingSet = [Math]::Max($cutHarnessPeakWorkingSet, $cutRunner.PeakWorkingSet64)
    $cutHarnessPeakPrivateBytes = [Math]::Max($cutHarnessPeakPrivateBytes, $cutRunner.PrivateMemorySize64)
    $cutSampleSum = 0L
    foreach ($cutServer in (Get-Process -Name 'laminardb-restore-test-stack' -ErrorAction SilentlyContinue)) {
        try {
            $cutServer.Refresh()
            $cutProcessId = $cutServer.Id
            $cutWorkingSet = $cutServer.WorkingSet64
            $cutPrivateBytes = $cutServer.PrivateMemorySize64
            $cutPeakWorkingSet = $cutServer.PeakWorkingSet64
            $cutSampleSum += $cutWorkingSet
            if (-not $cutProcesses.ContainsKey($cutProcessId)) {
                $cutProcesses[$cutProcessId] = [ordered]@{ pid = $cutProcessId; samples = 0L; observed_peak_working_set_bytes = 0L; max_sampled_private_bytes = 0L }
            }
            $cutEntry = $cutProcesses[$cutProcessId]
            $cutEntry.samples += 1
            $cutEntry.observed_peak_working_set_bytes = [Math]::Max($cutEntry.observed_peak_working_set_bytes, $cutPeakWorkingSet)
            $cutEntry.max_sampled_private_bytes = [Math]::Max($cutEntry.max_sampled_private_bytes, $cutPrivateBytes)
        } catch {
            # Process exit between enumeration and sampling does not change test correctness.
        }
    }
    $cutCombinedWorkingSet = [Math]::Max($cutCombinedWorkingSet, $cutSampleSum)
    $cutSampleCount += 1
    Start-Sleep -Milliseconds 1000
    $cutRunner.Refresh()
}
$cutRunner.WaitForExit()
$cutTestExit = $cutRunner.ExitCode
if ($null -eq $cutTestExit) { throw 'Harness exit status is unavailable' }
$cutResources = [ordered]@{
    started_utc = $cutRunStarted.ToString('o')
    ended_utc = [DateTime]::UtcNow.ToString('o')
    executable_sha256 = $env:LAMINAR_SOAK_LAMINARDB_SHA256
    test_exit_code = $cutTestExit
    sample_interval_ms = 1000
    samples = $cutSampleCount
    max_sampled_combined_working_set_bytes = $cutCombinedWorkingSet
    harness_observed_peak_working_set_bytes = $cutHarnessPeakWorkingSet
    harness_max_sampled_private_bytes = $cutHarnessPeakPrivateBytes
    processes = @($cutProcesses.Values | Sort-Object { $_.pid })
}
$cutResources | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $cutOutputDirectory 'restore-soak-01-resources.json') -Encoding utf8
Get-Content -LiteralPath (Join-Path $cutOutputDirectory 'restore-soak-01.stdout.txt') -Tail 15
Get-Content -LiteralPath (Join-Path $cutOutputDirectory 'restore-soak-01.stderr.txt') -Tail 25
exit $cutTestExit
