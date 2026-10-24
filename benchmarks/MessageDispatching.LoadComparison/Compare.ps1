param(
    [string] $Baseline = '9d1663c',
    [int] $Repeats = 3,
    [double] $DurationSeconds = 12,
    [string] $OutputDirectory = '',
    [string[]] $Scenarios = @('burst', 'partial-load', 'one-hot-key', 'heterogeneous', 'cpu-saturation')
)
$ErrorActionPreference = 'Stop'
function Get-ComparisonHash([string] $Path) {
    $algorithm = [Security.Cryptography.SHA256]::Create()
    $stream = [IO.File]::OpenRead($Path)
    try { return [BitConverter]::ToString($algorithm.ComputeHash($stream)).Replace('-', '') }
    finally { $stream.Dispose(); $algorithm.Dispose() }
}
if ($Repeats -lt 1 -or $DurationSeconds -lt 4) { throw 'Use at least one repeat and a duration of at least 4s.' }
$repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../..'))
if (!$OutputDirectory) { $OutputDirectory = Join-Path $PSScriptRoot 'results' }
$OutputDirectory = [IO.Path]::GetFullPath($OutputDirectory)
New-Item -ItemType Directory -Path $OutputDirectory -Force | Out-Null
$work = Join-Path $PSScriptRoot ('obj/comparison/' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $work -Force | Out-Null

Push-Location $repo
try {
    $commit = (& git rev-parse --verify "${Baseline}^{commit}").Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Cannot resolve baseline commit.' }
    $baselineSource = Join-Path $work 'baseline'
    # Materialize only tracked library sources. No worktree, checkout, or index mutations.
    $files = & git ls-tree -r --name-only $commit -- src/MessageDispatching
    foreach ($relative in $files) {
        if (!$relative.StartsWith('src/MessageDispatching/')) { throw 'Unexpected baseline source path.' }
        $destination = Join-Path $baselineSource $relative
        New-Item -ItemType Directory -Path (Split-Path $destination -Parent) -Force | Out-Null
        $content = & git show "${commit}:$relative"
        if ($LASTEXITCODE -ne 0) { throw "Cannot read $relative" }
        Set-Content -LiteralPath $destination -Value $content -Encoding utf8
    }

    & dotnet build (Join-Path $baselineSource 'src/MessageDispatching/MessageDispatching.csproj') -c Release --verbosity quiet
    if ($LASTEXITCODE -ne 0) { throw 'Baseline library build failed.' }
    & dotnet build (Join-Path $repo 'src/MessageDispatching/MessageDispatching.csproj') -c Release --verbosity quiet
    if ($LASTEXITCODE -ne 0) { throw 'Current library build failed.' }
    $assemblies = @{
        old = Join-Path $baselineSource 'src/MessageDispatching/bin/Release/net8.0/MessageDispatching.dll'
        new = Join-Path $repo 'src/MessageDispatching/bin/Release/net8.0/MessageDispatching.dll'
    }
    $runners = @{}
    foreach ($version in @('old', 'new')) {
        $out = Join-Path $work $version
        & dotnet build (Join-Path $PSScriptRoot 'MessageDispatching.LoadComparison.csproj') -c Release `
            "-p:DispatcherAssemblyPath=$($assemblies[$version])" -o $out --verbosity quiet
        if ($LASTEXITCODE -ne 0) { throw "$version comparison build failed." }
        $runners[$version] = Join-Path $out 'MessageDispatching.LoadComparison.dll'
    }
    $calibration = (& dotnet $runners['new'] --calibrate yes) | ConvertFrom-Json
    $passes = [int]$calibration.WorkPasses
    $capacity = [double]$calibration.Capacity
    if ($LASTEXITCODE -ne 0 -or $passes -lt 1 -or $capacity -le 0) { throw 'CPU work calibration failed.' }
    $metadata = [ordered]@{
        Baseline = $commit; Branch = (& git branch --show-current).Trim()
        DateUtc = [DateTime]::UtcNow.ToString('O'); Repeats = $Repeats
        DurationSeconds = $DurationSeconds; WorkPasses = $passes
        CalibratedCapacityPerSecond = $capacity
        OldAssemblySha256 = Get-ComparisonHash $assemblies['old']
        NewAssemblySha256 = Get-ComparisonHash $assemblies['new']
        HarnessSha256 = Get-ComparisonHash (Join-Path $PSScriptRoot 'Program.cs')
        LogicalProcessors = [Environment]::ProcessorCount
        Runtime = (& dotnet --list-runtimes) -join "`n"
        SampleMilliseconds = 100; ScaleDownSeconds = 1; TargetUtilization = 0.8
    }
    $metadata | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $OutputDirectory 'metadata.json') -Encoding utf8
    $runs = @()
    foreach ($scenario in $Scenarios) {
        for ($repeat = 1; $repeat -le $Repeats; $repeat++) {
            # Reverse the order every repeat to reduce thermal/order bias. Runs are sequential.
            $order = if ($repeat % 2 -eq 1) { @('old', 'new') } else { @('new', 'old') }
            $pair = @{}
            foreach ($version in $order) {
                $path = Join-Path $OutputDirectory "$scenario-$version-$repeat.json"
                $result = & dotnet $runners[$version] --scenario $scenario --version $version `
                    --output $path --work $passes --duration $DurationSeconds.ToString([Globalization.CultureInfo]::InvariantCulture) `
                    --capacity $capacity.ToString([Globalization.CultureInfo]::InvariantCulture)
                if ($LASTEXITCODE -ne 0) { throw "$scenario/$version/$repeat failed: $result" }
                $row = Get-Content -LiteralPath $path -Raw | ConvertFrom-Json
                $row | Add-Member -NotePropertyName Repeat -NotePropertyValue $repeat
                $runs += $row
                $pair[$version] = $row
                Write-Host ("{0,-15} {1} #{2}: throughput={3:F0}/s p95={4:F2}ms workerSeconds={5:F2} peak={6}" -f `
                    $scenario, $version, $repeat, $row.Throughput, $row.P95WaitMs, $row.WorkerSeconds, $row.PeakWorkers)
            }
            if ($pair.old.Enqueued -ne $pair.new.Enqueued -or $pair.old.Checksum -ne $pair.new.Checksum) {
                throw "$scenario/$repeat did not replay identical messages and CPU work."
            }
        }
    }
    $runs | Export-Csv -LiteralPath (Join-Path $OutputDirectory 'runs.csv') -NoTypeInformation -Encoding utf8
    $summary = foreach ($scenario in $Scenarios) {
        foreach ($version in @('old', 'new')) {
            $group = @($runs | Where-Object { $_.Scenario -eq $scenario -and $_.Version -eq $version })
            $values = [ordered]@{ Scenario = $scenario; Version = $version; Repeats = $Repeats }
            foreach ($metric in @('Throughput', 'P50WaitMs', 'P95WaitMs', 'P99WaitMs', 'WorkerSeconds', 'PeakWorkers', 'LateAverageWorkers', 'DrainSeconds')) {
                $sorted = @($group | ForEach-Object { $_.$metric } | Sort-Object)
                $middle = [int][Math]::Floor($sorted.Count / 2)
                $values[$metric] = if ($sorted.Count % 2) { $sorted[$middle] } else { ($sorted[$middle - 1] + $sorted[$middle]) / 2 }
            }
            [pscustomobject]$values
        }
    }
    $summary | Export-Csv -LiteralPath (Join-Path $OutputDirectory 'summary.csv') -NoTypeInformation -Encoding utf8
    $summary | Format-Table Scenario, Version, Throughput, P95WaitMs, WorkerSeconds, PeakWorkers, LateAverageWorkers -AutoSize
}
finally { Pop-Location }
