# Prints a markdown table of median ns/op, B/op and allocs/op for every
# benchmark in a "go test -bench -benchmem -count N" output file.
#
#   .\bench\microtable.ps1 bench\results\after.txt
param([Parameter(Mandatory)][string]$File)

function Get-Median([double[]]$v) {
    $s = @($v | Sort-Object)
    if ($s.Count % 2) { $s[[int](($s.Count - 1) / 2)] } else { ($s[$s.Count / 2 - 1] + $s[$s.Count / 2]) / 2 }
}

$pkg = ''
$rows = [ordered]@{}
foreach ($line in Get-Content $File) {
    if ($line -match '^pkg: (\S+)') { $pkg = $Matches[1] -replace '^ember/internal/', ''; continue }
    if ($line -match '^(Benchmark\S+?)(-\d+)?\s+\d+\s+([\d.]+) ns/op.*?([\d.]+) B/op\s+([\d.]+) allocs/op') {
        $key = "$pkg|$($Matches[1])"
        if (-not $rows.Contains($key)) { $rows[$key] = @{ ns = @(); b = @(); a = @() } }
        $rows[$key].ns += [double]$Matches[3]
        $rows[$key].b += [double]$Matches[4]
        $rows[$key].a += [double]$Matches[5]
    }
}
'| Package | Benchmark | ns/op | B/op | allocs/op |'
'|---|---|---:|---:|---:|'
foreach ($k in $rows.Keys) {
    $p, $n = $k.Split('|')
    $r = $rows[$k]
    '| {0} | {1} | {2:N1} | {3:N0} | {4:N0} |' -f $p, ($n -replace '^Benchmark', ''), (Get-Median $r.ns), (Get-Median $r.b), (Get-Median $r.a)
}
