#Requires -Version 5.1
<#
.SYNOPSIS
    Installs or removes the Bun preload module for atomic .claude.json writes.

.DESCRIPTION
    Layer 1 of the Config Broker's preventive approach: intercepts file writes
    at the Bun runtime level so .claude.json is always written atomically
    (temp file + rename), eliminating the truncation window that causes corruption.

    The installer:
    1. Copies claude-atomic-writes.js to ~/.claude/claude-atomic-writes.js
    2. Creates or updates ~/bunfig.toml with a [run] preload entry
    3. Preserves existing bunfig.toml content (line-based merge)

    If Claude Code was compiled with --no-compile-autoload-bunfig, the preload
    silently does nothing and Layer 2 (the broker daemon) remains active defense.

.PARAMETER Uninstall
    Remove the preload entry from bunfig.toml and delete the JS module.

.PARAMETER Test
    After install, verify the preload is picked up by Claude Code.

.PARAMETER Force
    Overwrite the JS module even if it already exists and is identical.

.EXAMPLE
    .\Install-AtomicWrites.ps1
    # Install the preload module

.EXAMPLE
    .\Install-AtomicWrites.ps1 -Test
    # Install and verify it works

.EXAMPLE
    .\Install-AtomicWrites.ps1 -Uninstall
    # Remove the preload module
#>

[CmdletBinding()]
param(
    [switch]$Uninstall,
    [switch]$Test,
    [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ============================================================================
# PATHS
# ============================================================================
$ScriptDir    = Split-Path -Parent $MyInvocation.MyCommand.Path
$SourceJs     = Join-Path $ScriptDir 'claude-atomic-writes.js'
$TargetDir    = Join-Path $env:USERPROFILE '.claude'
$TargetJs     = Join-Path $TargetDir 'claude-atomic-writes.js'
$BunfigPath   = Join-Path $env:USERPROFILE 'bunfig.toml'

# Absolute path with forward slashes for TOML compatibility
$PreloadPath  = ($TargetJs -replace '\\', '/')

# ============================================================================
# HELPERS
# ============================================================================

# BOM-less UTF-8 encoding for TOML files (PS 5.1 Out-File -Encoding UTF8 writes BOM)
$script:Utf8NoBom = [System.Text.UTF8Encoding]::new($false)

function Write-Status {
    param([string]$Message, [string]$Type = 'INFO')
    $color = switch ($Type) {
        'OK'    { 'Green' }
        'WARN'  { 'Yellow' }
        'ERROR' { 'Red' }
        default { 'Cyan' }
    }
    Write-Host "[$Type] $Message" -ForegroundColor $color
}

function Write-BunfigFile {
    <#
    .SYNOPSIS
        Writes lines to bunfig.toml using BOM-less UTF-8 (safe for Bun's TOML parser).
    #>
    param([string[]]$Lines)
    $content = ($Lines -join "`r`n") + "`r`n"
    [System.IO.File]::WriteAllText($BunfigPath, $content, $script:Utf8NoBom)
}

function Get-BunfigContent {
    if (Test-Path $BunfigPath) {
        $raw = Get-Content -Path $BunfigPath -Encoding UTF8
        if ($null -eq $raw) { return @() }
        return @($raw)
    }
    return @()
}

function Add-PreloadEntry {
    <#
    .SYNOPSIS
        Adds or updates the preload entry in bunfig.toml.
        Preserves existing content. Handles three cases:
        1. No bunfig.toml exists -> create new
        2. bunfig.toml exists but no [run] section -> append
        3. [run] section exists with/without preload -> update
    #>
    $lines = Get-BunfigContent
    $preloadValue = "`"$PreloadPath`""

    if ($lines.Count -eq 0) {
        # Case 1: Create new bunfig.toml
        Write-BunfigFile @('[run]', "preload = [$preloadValue]")
        Write-Status "Created $BunfigPath with preload entry." 'OK'
        return
    }

    # Find [run] section (allow optional whitespace inside brackets per TOML spec)
    $runSectionIdx = -1
    $preloadLineIdx = -1

    for ($i = 0; $i -lt $lines.Count; $i++) {
        $trimmed = $lines[$i].Trim()
        if ($trimmed -match '^\[\s*run\s*\]$') {
            $runSectionIdx = $i
        } elseif ($runSectionIdx -ge 0 -and $trimmed -match '^\[') {
            break
        } elseif ($runSectionIdx -ge 0 -and $trimmed -match '^preload\s*=') {
            $preloadLineIdx = $i
        }
    }

    if ($runSectionIdx -lt 0) {
        # Case 2: No [run] section -> append at end
        $lines += ''
        $lines += '[run]'
        $lines += "preload = [$preloadValue]"
        Write-BunfigFile $lines
        Write-Status "Added [run] section with preload to $BunfigPath." 'OK'
        return
    }

    if ($preloadLineIdx -ge 0) {
        # Case 3a: preload line exists -> append our path to the array
        $existingLine = $lines[$preloadLineIdx]
        # Only single-line arrays are supported; detect multi-line and warn
        if ($existingLine -notmatch 'preload\s*=\s*\[.*\]') {
            Write-Status "Multi-line preload array detected in $BunfigPath -- manual edit required." 'WARN'
            return
        }
        if ($existingLine -match 'preload\s*=\s*\[(.*)\]') {
            $existingEntries = $Matches[1].Trim()
            # Check if our path is already in the preload array (value-scoped, not substring)
            $parsedEntries = @([regex]::Matches($existingEntries, '"[^"]*"') | ForEach-Object { $_.Value })
            if ($parsedEntries -contains "`"$PreloadPath`"") {
                Write-Status "Preload entry already present in $BunfigPath." 'OK'
                return
            }
            if ($existingEntries) {
                $lines[$preloadLineIdx] = "preload = [$existingEntries, $preloadValue]"
            } else {
                $lines[$preloadLineIdx] = "preload = [$preloadValue]"
            }
        }
        Write-BunfigFile $lines
        Write-Status "Added preload path to existing preload array in $BunfigPath." 'OK'
    } else {
        # Case 3b: [run] section exists but no preload -> insert after [run]
        $insertIdx = $runSectionIdx + 1
        $before = $lines[0..$runSectionIdx]
        $after = if ($insertIdx -lt $lines.Count) { $lines[$insertIdx..($lines.Count - 1)] } else { @() }
        $lines = $before + @("preload = [$preloadValue]") + $after
        Write-BunfigFile $lines
        Write-Status "Added preload line to [run] section in $BunfigPath." 'OK'
    }
}

function Remove-PreloadEntry {
    <#
    .SYNOPSIS
        Removes our preload path from bunfig.toml.
        If the preload array becomes empty and [run] has no other settings, removes [run] too.
        Uses parse-and-rejoin (not regex surgery) to avoid producing malformed TOML.
    #>
    if (-not (Test-Path $BunfigPath)) {
        Write-Status "No bunfig.toml found at $BunfigPath -- nothing to remove." 'WARN'
        return
    }

    $lines = Get-BunfigContent
    $modified = $false

    # Scope removal to the [run] section only (other sections may have their own preload keys)
    $inRunSection = $false
    for ($i = 0; $i -lt $lines.Count; $i++) {
        $trimmed = $lines[$i].Trim()
        if ($trimmed -match '^\[\s*run\s*\]$') {
            $inRunSection = $true
            continue
        } elseif ($trimmed -match '^\[' -and $inRunSection) {
            # Entered a different section — stop looking
            break
        }
        if ($inRunSection -and $trimmed -match '^preload\s*=') {
            # Only single-line arrays are supported
            if ($lines[$i] -notmatch 'preload\s*=\s*\[.*\]') {
                Write-Status "Multi-line preload array detected -- manual edit required." 'WARN'
                return
            }
            if ($lines[$i] -match 'preload\s*=\s*\[(.*)\]') {
                $arrayContent = $Matches[1].Trim()
                # Quote-aware parse: extract all "..." entries to handle paths with commas
                $entries = @([regex]::Matches($arrayContent, '"[^"]*"') | ForEach-Object {
                    $_.Value
                } | Where-Object {
                    $_ -ne "`"$PreloadPath`""
                })

                if ($entries.Count -eq 0) {
                    # Array is now empty -- remove the line entirely
                    $lines[$i] = $null
                } else {
                    $lines[$i] = "preload = [$($entries -join ', ')]"
                }
                $modified = $true
            }
        }
    }

    if ($modified) {
        # Remove null entries and empty [run] sections
        $lines = @($lines | Where-Object { $null -ne $_ })

        # Check if [run] section is now empty (only header with no content before next section)
        $result = @()
        for ($i = 0; $i -lt $lines.Count; $i++) {
            $trimmed = $lines[$i].Trim()
            if ($trimmed -match '^\[\s*run\s*\]$') {
                # Check if next non-empty line is another section header or end
                $hasContent = $false
                for ($j = $i + 1; $j -lt $lines.Count; $j++) {
                    $nextTrimmed = $lines[$j].Trim()
                    if ($nextTrimmed -eq '') { continue }
                    if ($nextTrimmed -match '^\[') { break }
                    $hasContent = $true
                    break
                }
                if ($hasContent) {
                    $result += $lines[$i]
                }
                # else: skip empty [run] section
            } else {
                $result += $lines[$i]
            }
        }

        if ($result.Count -eq 0 -or ($result.Count -eq 1 -and $result[0].Trim() -eq '')) {
            # bunfig.toml is now empty -- delete it
            Remove-Item $BunfigPath -Force
            Write-Status "Removed empty $BunfigPath." 'OK'
        } else {
            Write-BunfigFile $result
            Write-Status "Removed preload entry from $BunfigPath." 'OK'
        }
    } else {
        Write-Status "Preload entry not found in $BunfigPath -- nothing to remove." 'WARN'
    }
}

function Test-PreloadActivation {
    <#
    .SYNOPSIS
        Tests whether the Bun preload is picked up by Claude Code.
        Creates a temporary probe preload that writes a marker file, runs claude --version,
        and checks if the marker was created.
    #>
    Write-Status 'Testing preload activation...'

    $claudeExe = Get-Command 'claude' -ErrorAction SilentlyContinue |
                 Select-Object -ExpandProperty Source -ErrorAction SilentlyContinue
    if (-not $claudeExe) {
        # Try well-known location
        $claudeExe = Join-Path $env:USERPROFILE '.local\bin\claude.exe'
        if (-not (Test-Path $claudeExe)) {
            Write-Status 'claude.exe not found in PATH or ~/.local/bin/. Cannot test preload.' 'ERROR'
            return $false
        }
    }

    $markerFile = Join-Path $env:TEMP "claude-preload-probe-$PID.txt"
    $probeJs = Join-Path $env:TEMP "claude-preload-probe-$PID.js"
    $origLines = Get-BunfigContent

    try {
        # Create probe module that writes a marker
        $probeCode = @"
const fs = require('fs');
fs.writeFileSync('$($markerFile -replace '\\', '/')', 'preload-active', 'utf8');
"@
        [System.IO.File]::WriteAllText($probeJs, $probeCode, $script:Utf8NoBom)

        # Add probe to existing bunfig (preserve original preload entries during test)
        $probePath = ($probeJs -replace '\\', '/')
        $probeValue = "`"$probePath`""
        if ($origLines.Count -gt 0) {
            $testLines = [System.Collections.ArrayList]::new([string[]]$origLines)
            $injected = $false
            $inRun = $false
            for ($idx = 0; $idx -lt $testLines.Count; $idx++) {
                $t = $testLines[$idx].Trim()
                if ($t -match '^\[\s*run\s*\]$') { $inRun = $true; continue }
                if ($inRun -and $t -match '^\[') { break }
                if ($inRun -and $t -match '^preload\s*=' -and $testLines[$idx] -notmatch '\[.*\]') {
                    Write-Status 'Multi-line preload array in bunfig.toml -- cannot inject probe safely. Skipping test.' 'WARN'
                    return $false
                }
                if ($inRun -and $testLines[$idx] -match 'preload\s*=\s*\[(.*)\]') {
                    $existing = $Matches[1].Trim()
                    if ($existing) {
                        $testLines[$idx] = "preload = [$existing, $probeValue]"
                    } else {
                        $testLines[$idx] = "preload = [$probeValue]"
                    }
                    $injected = $true
                    break
                }
            }
            if (-not $injected) {
                if (-not $inRun) {
                    $testLines.Add('') | Out-Null
                    $testLines.Add('[run]') | Out-Null
                }
                $testLines.Add("preload = [$probeValue]") | Out-Null
            }
            Write-BunfigFile ([string[]]$testLines)
        } else {
            Write-BunfigFile @('[run]', "preload = [$probeValue]")
        }

        # Run claude --version (fast, doesn't need auth)
        if (Test-Path $markerFile) { Remove-Item $markerFile -Force }
        $null = Start-Process -FilePath $claudeExe -ArgumentList '--version' `
            -WorkingDirectory $env:USERPROFILE -NoNewWindow -Wait -PassThru `
            -RedirectStandardOutput (Join-Path $env:TEMP "claude-probe-out-$PID.txt") `
            -RedirectStandardError  (Join-Path $env:TEMP "claude-probe-err-$PID.txt")

        if (Test-Path $markerFile) {
            Write-Status 'Preload is ACTIVE -- Bun autoloads bunfig.toml!' 'OK'
            return $true
        } else {
            Write-Status 'Preload NOT active -- Claude Code may have been compiled with --no-compile-autoload-bunfig.' 'WARN'
            Write-Status 'Layer 2 (broker daemon) will provide protection instead.' 'WARN'
            return $false
        }
    } finally {
        # Restore original bunfig FIRST (critical -- must happen even on crash/Ctrl+C)
        if ($origLines.Count -gt 0) {
            Write-BunfigFile $origLines
        } else {
            # We created bunfig.toml just for the test; delete the probe bunfig first
            # to avoid leaking the probe path into the final preload array
            if (Test-Path $BunfigPath) { Remove-Item $BunfigPath -Force }
            Add-PreloadEntry
        }

        # Cleanup probe files
        foreach ($f in @($markerFile, $probeJs,
                         (Join-Path $env:TEMP "claude-probe-out-$PID.txt"),
                         (Join-Path $env:TEMP "claude-probe-err-$PID.txt"))) {
            if (Test-Path $f) { Remove-Item $f -Force -ErrorAction SilentlyContinue }
        }
    }
}

# ============================================================================
# MAIN
# ============================================================================
if ($Uninstall) {
    Write-Host ''
    Write-Host '  Uninstalling Claude Atomic Writes preload...' -ForegroundColor Cyan
    Write-Host ''

    # Detect and clean up stale probe bunfig left by a crashed -Test run
    if (Test-Path $BunfigPath) {
        $bunfigText = [System.IO.File]::ReadAllText($BunfigPath)
        if ($bunfigText -match 'claude-preload-probe-') {
            Write-Status "Detected stale probe bunfig from crashed -Test run -- removing." 'WARN'
            Remove-Item $BunfigPath -Force
        }
    }

    Remove-PreloadEntry

    if (Test-Path $TargetJs) {
        Remove-Item $TargetJs -Force
        Write-Status "Removed $TargetJs." 'OK'
    } else {
        Write-Status "JS module not found at $TargetJs -- already removed." 'WARN'
    }

    Write-Host ''
    Write-Status 'Uninstall complete.' 'OK'
    exit 0
}

# Install flow
Write-Host ''
Write-Host '  Installing Claude Atomic Writes preload...' -ForegroundColor Cyan
Write-Host "  Source: $SourceJs" -ForegroundColor DarkGray
Write-Host "  Target: $TargetJs" -ForegroundColor DarkGray
Write-Host "  Config: $BunfigPath" -ForegroundColor DarkGray
Write-Host ''

# Validate source exists
if (-not (Test-Path $SourceJs)) {
    Write-Status "Source file not found: $SourceJs" 'ERROR'
    exit 1
}

# Ensure target directory exists
if (-not (Test-Path $TargetDir)) {
    New-Item -ItemType Directory -Path $TargetDir -Force | Out-Null
}

# Copy JS module
$needsCopy = $true
if (-not $Force -and (Test-Path $TargetJs)) {
    $srcHash = (Get-FileHash $SourceJs -Algorithm SHA256).Hash
    $dstHash = (Get-FileHash $TargetJs -Algorithm SHA256).Hash
    if ($srcHash -eq $dstHash) {
        Write-Status 'JS module is up-to-date (identical hash).' 'OK'
        $needsCopy = $false
    }
}

if ($needsCopy) {
    Copy-Item -Path $SourceJs -Destination $TargetJs -Force
    Write-Status "Copied JS module to $TargetJs." 'OK'
}

# Configure bunfig.toml
Add-PreloadEntry

Write-Host ''
Write-Status 'Install complete.' 'OK'

if ($Test) {
    Write-Host ''
    $result = Test-PreloadActivation
    exit ([int](-not $result))
}
