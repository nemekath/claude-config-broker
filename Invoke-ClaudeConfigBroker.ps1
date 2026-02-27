#Requires -Version 5.1
<#
.SYNOPSIS
    Config Broker Daemon v1.2 -- a "reverse JSON proxy" that serializes concurrent
    access to ~/.claude.json using a system-wide Named Mutex and JSON deep-merge.
    Compatible with PowerShell 5.1 (.NET Framework) and PowerShell 7+ (.NET 6/8/9).

.DESCRIPTION
    Claude Code instances all write to ~/.claude.json without locking, causing
    corruption when multiple sessions run in parallel. This daemon acts as a
    post-write serializer using event-based FileSystemWatcher monitoring.

    Key features:
    - Event-based file monitoring (Register-ObjectEvent) with debounce
    - Gap detection fallback for missed FSW events
    - Named Mutex serialization (system-wide, cross-session)
    - Atomic writes via temp-file + rename pattern
    - Shadow state persistence (RAM + disk) for crash recovery
    - Deep-merge engine preserving changes from concurrent sessions
    - Single-instance guard (only one broker per machine)
    - Log rotation and stale temp-file cleanup
    - Cross-runtime: PS 5.1 (.NET Framework) + PS 7+ (.NET 6/8/9)

    LIMITATION (Deep-Merge property deletion):
    When Claude CLI intentionally deletes a JSON property, DeepMerge will restore
    it from the shadow state because no common ancestor is available (two-way merge).
    If this causes issues, use -MergeStrategy LastValidWins to disable merging.
    The broker logs a WARN when shadow-only properties are preserved.

    Architecture:

    +----------------+     +----------------+
    | Claude CLI 1   |     | Claude CLI 2   |    ... N instances
    +-------+--------+     +-------+--------+
            |  write               |  write
            v                      v
    +--------------------------------------+
    |        .claude.json (NTFS)           |  <-- Race condition zone
    +------------------+-------------------+
                       |  FileSystemWatcher (event-based)
                       v
    +--------------------------------------+
    |       Config Broker Daemon           |
    |  +--------------------------------+  |
    |  |  Named Mutex Lock              |  |
    |  |  JSON Validate + Deep Merge    |  |
    |  |  Atomic Write-Back             |  |
    |  |  Shadow State (RAM + Disk)     |  |
    |  +--------------------------------+  |
    +--------------------------------------+

    The daemon detects changes within milliseconds via FSW events, validates JSON,
    merges with shadow state if needed, and writes back atomically. A gap detection
    fallback (timestamp-based) covers the rare case of missed FSW events.

    Files:
    - ~/.claude.json                              Monitored config file
    - ~/.claude/broker.log                        Daemon log (rotated)
    - ~/.claude/broker.pid                        PID file for external tools
    - ~/.claude/backups/.claude.json.broker-shadow Persisted shadow state

.PARAMETER CleanupDays
    Remove .corrupted.* backup files older than this many days (default: 7).

.PARAMETER DebounceMs
    Milliseconds to wait after the last file event before processing (default: 100).
    Reduced from 300ms in v1.1 for faster reactive response. Safe without Layer 1
    because Read-JsonSafe validates JSON and retries once after 50ms on parse failure;
    corrupt reads trigger a restore from shadow state, never silent data loss.
    Prevents multiple processing runs during rapid successive writes.

.PARAMETER GapCheckMs
    Timeout in milliseconds for gap detection fallback (default: 2000).
    When no FSW event arrives within this window, the daemon checks the file's
    LastWriteTimeUtc to catch any missed changes. Lower values detect gaps faster
    but increase idle CPU usage slightly. Rounded up to whole seconds internally
    (Wait-Event -Timeout accepts seconds only).

.PARAMETER MutexTimeoutMs
    Max wait time in milliseconds to acquire the named mutex (default: 5000).

.PARAMETER LogFile
    Path to the log file. Defaults to ~/.claude/broker.log

.PARAMETER LogMaxSizeMB
    Maximum log file size in MB before rotation (default: 10). One rotated
    generation is kept (.log.1). Checked every 6 hours.

.PARAMETER MergeStrategy
    How to handle concurrent changes: 'DeepMerge' (default) or 'LastValidWins'.
    DeepMerge preserves changes from both the shadow state and the new write.
    LastValidWins simply keeps whichever valid state is newer (no merge, safer
    against the property-deletion limitation described above).

.EXAMPLE
    # Standard usage - run before starting Claude Code sessions
    .\Invoke-ClaudeConfigBroker.ps1

.EXAMPLE
    # Conservative mode - no merging, just corruption protection
    .\Invoke-ClaudeConfigBroker.ps1 -MergeStrategy LastValidWins

.EXAMPLE
    # Faster gap detection + smaller log file
    .\Invoke-ClaudeConfigBroker.ps1 -GapCheckMs 1000 -LogMaxSizeMB 5

.EXAMPLE
    # As a background job
    Start-Job -FilePath .\Invoke-ClaudeConfigBroker.ps1
#>

[CmdletBinding()]
param(
    [ValidateRange(1, 365)]
    [int]$CleanupDays     = 7,

    [ValidateRange(50, 10000)]
    [int]$DebounceMs      = 100,

    [ValidateRange(500, 30000)]
    [int]$GapCheckMs      = 2000,

    [ValidateRange(1000, 60000)]
    [int]$MutexTimeoutMs  = 5000,

    [string]$LogFile      = (Join-Path $env:USERPROFILE '.claude\broker.log'),

    [ValidateRange(1, 100)]
    [int]$LogMaxSizeMB    = 10,

    [ValidateSet('DeepMerge', 'LastValidWins')]
    [string]$MergeStrategy = 'DeepMerge'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# =============================================================================
# PATHS
# =============================================================================
$ConfigFile   = Join-Path $env:USERPROFILE '.claude.json'
$BackupDir    = Join-Path $env:USERPROFILE '.claude\backups'
$ShadowFile   = Join-Path $env:USERPROFILE '.claude\backups\.claude.json.broker-shadow'
$WatchDir     = $env:USERPROFILE
$WatchFilter  = '.claude.json'
$MutexName    = 'Global\ClaudeConfigBrokerMutex'
$PidFile      = Join-Path $env:USERPROFILE '.claude\broker.pid'

if (-not (Test-Path $BackupDir)) {
    New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null
}

# Clean up stale temp files from previous hard crashes (kill -9, BSOD)
# broker-tmp files are only created by this broker (single-instance guard ensures no race)
Get-ChildItem -Path $WatchDir -Filter '.claude.json.broker-tmp.*' -ErrorAction SilentlyContinue |
    Remove-Item -Force -ErrorAction SilentlyContinue
# broker-atomic files are created by the JS preload in Claude Code processes — only delete
# files older than 10 seconds to avoid racing with an in-flight atomic write
# Use UTC to avoid DST-transition edge cases
$atomicCutoff = [DateTime]::UtcNow.AddSeconds(-10)
Get-ChildItem -Path $WatchDir -Filter '.claude.json.broker-atomic.*' -ErrorAction SilentlyContinue |
    Where-Object { $_.LastWriteTimeUtc -lt $atomicCutoff } |
    Remove-Item -Force -ErrorAction SilentlyContinue

# =============================================================================
# SINGLE INSTANCE GUARD
# =============================================================================
$instanceMutex = $null
try {
    $instanceMutex = [System.Threading.Mutex]::new($false, 'Global\ClaudeConfigBrokerInstance')
    if (-not $instanceMutex.WaitOne(0)) {
        Write-Host '[BROKER] Another instance is already running. Exiting.' -ForegroundColor Yellow
        exit 0
    }
} catch [System.Threading.AbandonedMutexException] {
    # Previous instance crashed -- catching this exception means the mutex
    # is now owned by this thread (standard .NET behavior). We take over.
}

# Write PID file for external tools (no trailing newline, no BOM)
[System.IO.File]::WriteAllText($PidFile, $PID.ToString())

# BOM-less UTF-8 encoding (PS 5.1 Out-File -Encoding utf8 writes BOM)
$script:Utf8NoBom = [System.Text.UTF8Encoding]::new($false)

# =============================================================================
# LOGGING
# =============================================================================
function Invoke-LogRotation {
    try {
        if (-not (Test-Path $LogFile)) { return }
        $sizeMB = (Get-Item $LogFile).Length / 1MB
        if ($sizeMB -ge $LogMaxSizeMB) {
            $rotated = "$LogFile.1"
            # Keep only one rotated generation
            if (Test-Path $rotated) { Remove-Item $rotated -Force -ErrorAction SilentlyContinue }
            Move-FileAtomic $LogFile $rotated
        }
    } catch {
        # Log rotation is non-critical — log continues growing until next attempt
        Write-Host "[$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff')] [WARN] Log rotation failed: $($_.Exception.Message)" -ForegroundColor Yellow
    }
}

function Write-Log {
    param([string]$Message, [string]$Level = 'INFO')
    $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss.fff'
    $entry = "[$ts] [$Level] $Message"
    $color = switch ($Level) {
        'ERROR'   { 'Red' }
        'WARN'    { 'Yellow' }
        'MERGE'   { 'Cyan' }
        'RESTORE' { 'Green' }
        'LOCK'    { 'DarkGray' }
        default   { 'Gray' }
    }
    Write-Host $entry -ForegroundColor $color
    try { [System.IO.File]::AppendAllText($LogFile, "$entry`r`n", $script:Utf8NoBom) } catch { }
}

# =============================================================================
# NAMED MUTEX WRAPPER
# =============================================================================
$configMutex = [System.Threading.Mutex]::new($false, $MutexName)
$script:ConfigMutexHeld = $false

function Invoke-WithLock {
    <#
    .SYNOPSIS
        Executes a scriptblock while holding the named mutex.
        Returns $null if the lock cannot be acquired within the timeout.
    #>
    param(
        [scriptblock]$Action,
        [string]$Description = 'operation'
    )

    $acquired = $false
    try {
        $acquired = $configMutex.WaitOne($MutexTimeoutMs)
        if (-not $acquired) {
            Write-Log "Mutex timeout (${MutexTimeoutMs}ms) for: $Description" 'WARN'
            return $null
        }
        $script:ConfigMutexHeld = $true
        Write-Log "Lock acquired: $Description" 'LOCK'
        return & $Action
    } catch [System.Threading.AbandonedMutexException] {
        # Another process holding the mutex crashed -- we inherit it
        $acquired = $true
        $script:ConfigMutexHeld = $true
        Write-Log "Inherited abandoned mutex for: $Description" 'WARN'
        return & $Action
    } finally {
        if ($acquired) {
            try { $configMutex.ReleaseMutex() } catch { }
            $script:ConfigMutexHeld = $false
            Write-Log "Lock released: $Description" 'LOCK'
        }
    }
}

# =============================================================================
# CROSS-RUNTIME ATOMIC FILE MOVE (PS 5.1 + PS 7)
# =============================================================================
# Uses File.Replace (Win32 ReplaceFile) when destination exists — atomically
# swaps source into destination with no race window. Falls back to File.Move
# only when destination doesn't exist yet (first shadow persist, first log, etc.).

function Move-FileAtomic {
    <#
    .SYNOPSIS
        Moves src to dst, overwriting dst atomically if it exists.
        Uses File.Replace() (Win32 ReplaceFile) for atomic replacement,
        File.Move() only when destination doesn't exist yet.
        On IOException, retries once after a brief pause. If still failing,
        propagates the error — callers handle cleanup (temp file + shadow state).
    #>
    param([string]$Source, [string]$Destination)
    if ([System.IO.File]::Exists($Destination)) {
        try {
            # File.Replace: atomically replaces Destination with Source contents.
            # Third parameter (NullString) skips backup. Must use [NullString]::Value
            # instead of $null — PS 5.1 auto-converts $null to "" which is invalid.
            # Also preserves destination's ACLs and alternate data streams.
            [System.IO.File]::Replace($Source, $Destination, [NullString]::Value)
        } catch [System.IO.IOException] {
            # Sharing violation — another process holds a lock. Retry once after
            # a brief pause (the lock is typically released within milliseconds).
            # If it still fails, let the exception propagate — the caller's finally
            # block cleans up the temp file and the destination remains unchanged.
            Start-Sleep -Milliseconds 50
            [System.IO.File]::Replace($Source, $Destination, [NullString]::Value)
        }
    } else {
        [System.IO.File]::Move($Source, $Destination)
    }
}

# =============================================================================
# JSON UTILITIES
# =============================================================================
function Read-JsonSafe {
    <#
    .SYNOPSIS
        Reads and parses a JSON file. Returns $null on any failure.
        Uses FileShare.ReadWrite to tolerate concurrent writers (Claude CLI).
    #>
    param([string]$Path)
    [System.IO.FileStream]$fs = $null
    [System.IO.StreamReader]$sr = $null
    try {
        # Single syscall: FileStream with ReadWrite sharing prevents sharing violations
        $fs = [System.IO.FileStream]::new(
            $Path,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::Read,
            [System.IO.FileShare]([System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete))
        if ($fs.Length -eq 0) { return $null }
        # leaveOpen: $true — StreamReader must not close the FileStream prematurely
        $sr = [System.IO.StreamReader]::new($fs, [System.Text.Encoding]::UTF8, $true, 4096, $true)
        $raw = $sr.ReadToEnd()
        if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
        return ($raw | ConvertFrom-Json -ErrorAction Stop)
    } catch {
        return $null
    } finally {
        if ($sr) { $sr.Dispose() }
        if ($fs) { $fs.Dispose() }
    }
}

function Write-JsonAtomic {
    <#
    .SYNOPSIS
        Writes a JSON object to a file using temp-file + rename for atomicity.
        MUST be called from within Invoke-WithLock.
        Pass -Json to skip serialization if the caller already has the JSON string.
    #>
    param(
        [string]$Path,
        [object]$Data,
        [string]$Json = $null
    )

    $tempPath = "$Path.broker-tmp.$PID"
    try {
        if (-not $Json) { $Json = $Data | ConvertTo-Json -Depth 50 -Compress:$false }

        # Validate JSON in memory (avoids disk re-read roundtrip ~1-2ms)
        $null = $Json | ConvertFrom-Json -ErrorAction Stop

        [System.IO.File]::WriteAllText($tempPath, $Json, $script:Utf8NoBom)

        # Atomic replace (cross-runtime: PS 5.1 + PS 7)
        Move-FileAtomic $tempPath $Path
    } finally {
        if (Test-Path $tempPath) {
            Remove-Item $tempPath -Force -ErrorAction SilentlyContinue
        }
    }
}

# =============================================================================
# DEEP MERGE ENGINE
# =============================================================================
function Merge-JsonDeep {
    <#
    .SYNOPSIS
        Deep-merges two PSCustomObjects. Properties from $Incoming override $Base,
        but nested objects are recursively merged rather than replaced wholesale.
        Arrays are replaced (not concatenated) since Claude config arrays represent
        complete state, not append-only lists.

    .DESCRIPTION
        Merge strategy:
        - Scalar values: $Incoming wins
        - Nested objects: recursive merge
        - Arrays: $Incoming wins (complete replacement)
        - Properties only in $Base: preserved
        - Properties only in $Incoming: added
    #>
    param(
        [Parameter(Mandatory)]$Base,
        [Parameter(Mandatory)]$Incoming
    )

    if ($null -eq $Base) { return $Incoming }
    if ($null -eq $Incoming) { return $Base }

    # If either is not an object (scalar/array), incoming wins
    $baseIsObject    = $Base -is [System.Management.Automation.PSCustomObject]
    $incomingIsObject = $Incoming -is [System.Management.Automation.PSCustomObject]

    if (-not $baseIsObject -or -not $incomingIsObject) {
        return $Incoming
    }

    # Deep merge objects
    $result = [PSCustomObject]@{}

    # Start with all properties from base
    foreach ($prop in $Base.PSObject.Properties) {
        $result | Add-Member -NotePropertyName $prop.Name -NotePropertyValue $prop.Value -Force
    }

    # Overlay / merge incoming properties
    foreach ($prop in $Incoming.PSObject.Properties) {
        $incomingVal = $prop.Value
        $baseVal = $null

        if ($null -ne $result.PSObject.Properties[$prop.Name]) {
            $baseVal = $result.$($prop.Name)
        }

        if ($null -ne $baseVal -and
            $baseVal -is [System.Management.Automation.PSCustomObject] -and
            $incomingVal -is [System.Management.Automation.PSCustomObject]) {
            # Recursive merge for nested objects
            $merged = Merge-JsonDeep -Base $baseVal -Incoming $incomingVal
            $result | Add-Member -NotePropertyName $prop.Name -NotePropertyValue $merged -Force
        } else {
            # Scalar, array, or type mismatch: incoming wins
            $result | Add-Member -NotePropertyName $prop.Name -NotePropertyValue $incomingVal -Force
        }
    }

    return $result
}

function ConvertTo-NormalizedJson {
    <#
    .SYNOPSIS
        Serializes an object to JSON with recursively sorted property keys.
        Ensures deterministic output for comparison regardless of property order.
    #>
    param([Parameter(Mandatory)]$Object)

    function Sort-Properties {
        param($Obj)
        if ($null -eq $Obj) { return $null }
        if ($Obj -is [System.Management.Automation.PSCustomObject]) {
            $sorted = [ordered]@{}
            foreach ($prop in ($Obj.PSObject.Properties | Sort-Object Name)) {
                $sorted[$prop.Name] = Sort-Properties $prop.Value
            }
            return [PSCustomObject]$sorted
        }
        if ($Obj -is [System.Collections.IEnumerable] -and $Obj -isnot [string]) {
            # Use foreach instead of pipeline to preserve $null array elements
            $arr = [System.Collections.ArrayList]::new()
            foreach ($item in $Obj) { $arr.Add((Sort-Properties $item)) | Out-Null }
            return @($arr)
        }
        return $Obj
    }

    $normalized = Sort-Properties $Object
    return ($normalized | ConvertTo-Json -Depth 50 -Compress)
}

# =============================================================================
# SHADOW STATE MANAGEMENT
# =============================================================================
$script:ShadowState = $null
$script:LastPersistedShadowJson = ''

function Update-ShadowState {
    param([object]$State, [string]$Json = $null)
    $script:ShadowState = $State

    if (-not $Json) { $Json = $State | ConvertTo-Json -Depth 50 }

    # Skip disk persist if content is identical to last persisted shadow
    if ($Json -eq $script:LastPersistedShadowJson) { return }

    # Persist shadow to disk atomically (non-critical, best-effort)
    try {
        $shadowTmp = "$ShadowFile.tmp.$PID"
        [System.IO.File]::WriteAllText($shadowTmp, $Json, $script:Utf8NoBom)
        Move-FileAtomic $shadowTmp $ShadowFile
        $script:LastPersistedShadowJson = $Json
    } catch {
        Write-Log "Shadow persist failed: $($_.Exception.Message)" 'WARN'
        # Cleanup orphaned temp file
        if (Test-Path $shadowTmp) { Remove-Item $shadowTmp -Force -ErrorAction SilentlyContinue }
    }
}

function Get-ShadowState {
    if ($null -ne $script:ShadowState) {
        return $script:ShadowState
    }
    # Try loading from disk
    $fromDisk = Read-JsonSafe $ShadowFile
    if ($null -ne $fromDisk) {
        $script:ShadowState = $fromDisk
        Write-Log 'Shadow state loaded from disk.'
        return $fromDisk
    }
    return $null
}

# =============================================================================
# CORE: PROCESS CONFIG CHANGE
# =============================================================================
function Invoke-ProcessConfigChange {
    Invoke-WithLock -Description 'process config change' -Action {

        $current = Read-JsonSafe $ConfigFile

        # Retry once if null — file might still be mid-write (replaces static Sleep)
        if ($null -eq $current) {
            Start-Sleep -Milliseconds 50
            $current = Read-JsonSafe $ConfigFile
        }

        if ($null -ne $current) {
            # -- File is valid JSON ------------------------------------
            $shadow = Get-ShadowState

            if ($MergeStrategy -eq 'DeepMerge' -and $null -ne $shadow) {
                # Merge shadow + incoming to preserve changes from both
                $merged = Merge-JsonDeep -Base $shadow -Incoming $current

                # Count shadow-only properties (may be intentional deletions by CLI)
                $baseOnlyCount = @($shadow.PSObject.Properties.Name | Where-Object {
                    $null -eq $current.PSObject.Properties[$_]
                }).Count
                if ($baseOnlyCount -gt 0) {
                    Write-Log "DeepMerge: $baseOnlyCount top-level shadow-only property/ies preserved (may be intentional deletions). Use -MergeStrategy LastValidWins to avoid." 'WARN'
                }

                # Fast-path: shadow-only properties exist → merge definitely differs
                if ($baseOnlyCount -gt 0) {
                    $changed = $true
                } else {
                    # Same top-level keys — deep compare via normalized JSON
                    $changed = (ConvertTo-NormalizedJson $merged) -ne (ConvertTo-NormalizedJson $current)
                }

                if ($changed) {
                    # Serialize once, reuse for config write + shadow persist
                    $mergedJson = $merged | ConvertTo-Json -Depth 50 -Compress:$false
                    Write-JsonAtomic -Path $ConfigFile -Data $merged -Json $mergedJson

                    Update-ShadowState $merged -Json $mergedJson
                    Write-Log "Deep-merged shadow + incoming -> wrote merged config." 'MERGE'
                    $script:Stats.Merges++
                } else {
                    $currentJson = $current | ConvertTo-Json -Depth 50 -Compress:$false
                    Update-ShadowState $current -Json $currentJson
                    $script:Stats.Snapshots++
                }
            } else {
                # LastValidWins or no shadow yet -- just snapshot
                Update-ShadowState $current
                $script:Stats.Snapshots++
            }
        } else {
            # -- File is corrupt ---------------------------------------
            $script:Stats.Corruptions++
            Write-Log "CORRUPTION DETECTED (#$($script:Stats.Corruptions))!" 'ERROR'

            $shadow = Get-ShadowState
            if ($null -ne $shadow) {
                Write-JsonAtomic -Path $ConfigFile -Data $shadow
                Write-Log 'Restored from shadow state (RAM).' 'RESTORE'
                $script:Stats.Restores++
                return
            }

            # Fallback: search Claude Code's own backups
            [array]$backups = @(Get-ChildItem -Path $BackupDir -Filter '.claude.json.backup.*' -ErrorAction SilentlyContinue |
                Sort-Object LastWriteTimeUtc -Descending)

            foreach ($backup in $backups) {
                $data = Read-JsonSafe $backup.FullName
                if ($null -ne $data) {
                    Write-JsonAtomic -Path $ConfigFile -Data $data

                    Update-ShadowState $data
                    Write-Log "Restored from Claude backup: $($backup.Name)" 'RESTORE'
                    $script:Stats.Restores++
                    return
                }
            }

            Write-Log 'NO VALID BACKUP FOUND -- manual intervention required!' 'ERROR'
        }
    }
}

# =============================================================================
# CLEANUP
# =============================================================================
function Invoke-BackupCleanup {
    $cutoff = [DateTime]::UtcNow.AddDays(-$CleanupDays)
    [array]$stale = @(Get-ChildItem -Path $BackupDir -Filter '.claude.json.corrupted.*' -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTimeUtc -lt $cutoff })
    if ($stale.Count -gt 0) {
        $stale | Remove-Item -Force -ErrorAction SilentlyContinue
        Write-Log "Cleaned up $($stale.Count) stale corrupted backups."
    }
}

# =============================================================================
# STARTUP
# =============================================================================
$banner = @"

  +---------------------------------------------------------+
  |        Claude Config Broker Daemon v1.2                 |
  |        "Reverse JSON Proxy" for .claude.json            |
  +---------------------------------------------------------+
    Mutex:     $MutexName
    Config:    $ConfigFile
    Shadow:    $ShadowFile
    Strategy:  $MergeStrategy
    Debounce:  ${DebounceMs}ms
    GapCheck:  ${GapCheckMs}ms
    LogMax:    ${LogMaxSizeMB} MB
  +---------------------------------------------------------+

"@
Write-Host $banner -ForegroundColor Cyan

$script:Stats = @{
    Corruptions = 0
    Restores    = 0
    Merges      = 0
    Snapshots   = 0
}
$script:RuntimeTimer = [System.Diagnostics.Stopwatch]::StartNew()

# Initial load
Write-Log 'Initializing...'
Invoke-WithLock -Description 'initial load' -Action {
    $initial = Read-JsonSafe $ConfigFile
    if ($null -ne $initial) {
        Update-ShadowState $initial
        Write-Log 'Initial config loaded into shadow state.'
    } else {
        Write-Log 'Initial config is corrupt -- attempting restore...' 'WARN'
    }
}

if ($null -eq (Get-ShadowState)) {
    Invoke-ProcessConfigChange
}
if ($null -eq (Get-ShadowState)) {
    Write-Log 'DAEMON RUNNING WITHOUT SHADOW STATE -- will recover on next valid write.' 'WARN'
}

# Initialize last known write time for watcher gap detection
# FileInfo.LastWriteTimeUtc returns 1601-01-01 for non-existent files (safe default)
$script:LastKnownWriteTime = ([System.IO.FileInfo]::new($ConfigFile)).LastWriteTimeUtc

Invoke-BackupCleanup
Invoke-LogRotation

# =============================================================================
# FILE SYSTEM WATCHER (Event-based, more reliable than WaitForChanged)
# =============================================================================
$watcher = [System.IO.FileSystemWatcher]::new()
$watcher.Path                  = $WatchDir
$watcher.Filter                = $WatchFilter
$watcher.NotifyFilter          = [System.IO.NotifyFilters]::LastWrite -bor
                                 [System.IO.NotifyFilters]::Size -bor
                                 [System.IO.NotifyFilters]::FileName
$watcher.InternalBufferSize    = 65536  # 64 KB — home dir can be busy

# Register event subscriptions (queued mode — events go to PS event queue)
foreach ($evtName in @('Changed', 'Created', 'Renamed', 'Deleted')) {
    Register-ObjectEvent -InputObject $watcher -EventName $evtName `
        -SourceIdentifier "BrokerFSW_$evtName" | Out-Null
}
$watcher.EnableRaisingEvents = $true

# Periodic cleanup timer
$cleanupTimer = [System.Diagnostics.Stopwatch]::StartNew()
$cleanupIntervalMs = 6 * 60 * 60 * 1000

Write-Log 'Broker is active. Press Ctrl+C to stop.'
Write-Host ''

# =============================================================================
# MAIN LOOP (Event-based with debounce + gap detection fallback)
# =============================================================================
$gapTimeoutSec = [math]::Max(1, [math]::Ceiling($GapCheckMs / 1000))
try {
    while ($true) {
        # Idle wait: block until an event arrives or gap-check timeout
        $ev = Wait-Event -Timeout $gapTimeoutSec

        if ($ev -and $ev.SourceIdentifier -like 'BrokerFSW_*') {
            # FSW event received — drain remaining FSW events (preserve non-FSW events)
            Remove-Event -EventIdentifier $ev.EventIdentifier -ErrorAction SilentlyContinue
            Get-Event -ErrorAction SilentlyContinue |
                Where-Object { $_.SourceIdentifier -like 'BrokerFSW_*' } | Remove-Event

            # Debounce: wait for writes to settle (monotonic clock, immune to DST/NTP)
            $debounceTimer = [System.Diagnostics.Stopwatch]::StartNew()
            while ($debounceTimer.ElapsedMilliseconds -lt $DebounceMs) {
                Start-Sleep -Milliseconds 50
                $pending = @(Get-Event -ErrorAction SilentlyContinue |
                    Where-Object { $_.SourceIdentifier -like 'BrokerFSW_*' })
                if ($pending.Count -gt 0) {
                    $pending | Remove-Event
                    # New events arrived — reset debounce window
                    $debounceTimer.Restart()
                }
            }

            # Debounce settled — process (catch transient errors to keep daemon alive)
            try {
                Invoke-ProcessConfigChange
            } catch {
                Write-Log "Config processing failed (will retry on next event): $($_.Exception.Message)" 'ERROR'
            }
            $script:LastKnownWriteTime = ([System.IO.FileInfo]::new($ConfigFile)).LastWriteTimeUtc
        } else {
            # Remove non-FSW event to prevent busy-spin (Wait-Event peeks, doesn't consume)
            if ($ev) {
                Remove-Event -EventIdentifier $ev.EventIdentifier -ErrorAction SilentlyContinue
            }
            # Timeout or non-FSW event — gap detection fallback
            $currentWriteTime = ([System.IO.FileInfo]::new($ConfigFile)).LastWriteTimeUtc
            if ($currentWriteTime -gt $script:LastKnownWriteTime) {
                Write-Log 'Detected missed file change (watcher gap recovery).' 'WARN'
                try {
                    Invoke-ProcessConfigChange
                } catch {
                    Write-Log "Config processing failed (will retry on next event): $($_.Exception.Message)" 'ERROR'
                }
                $script:LastKnownWriteTime = ([System.IO.FileInfo]::new($ConfigFile)).LastWriteTimeUtc
            }
        }

        # Periodic cleanup + log rotation
        if ($cleanupTimer.ElapsedMilliseconds -ge $cleanupIntervalMs) {
            Invoke-BackupCleanup
            Invoke-LogRotation
            $cleanupTimer.Restart()
        }
    }
} catch {
    if ($_.Exception -is [System.Management.Automation.PipelineStoppedException] -or
        $_.Exception.InnerException -is [System.OperationCanceledException]) {
        # Ctrl+C -- graceful shutdown
    } else {
        Write-Log "Fatal error: $($_.Exception.Message)" 'ERROR'
        throw
    }
} finally {
    # Unregister FSW event subscriptions
    Get-EventSubscriber | Where-Object { $_.SourceIdentifier -like 'BrokerFSW_*' } |
        Unregister-Event -ErrorAction SilentlyContinue
    $watcher.EnableRaisingEvents = $false
    $watcher.Dispose()
    if ($script:ConfigMutexHeld) {
        try { $configMutex.ReleaseMutex() } catch { }
    }
    $configMutex.Dispose()

    # Remove PID file
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue

    if ($null -ne $instanceMutex) {
        try { $instanceMutex.ReleaseMutex() } catch { }
        $instanceMutex.Dispose()
    }

    $script:RuntimeTimer.Stop()
    $runtime = $script:RuntimeTimer.Elapsed
    Write-Host ''
    Write-Log '==========================================================='
    Write-Log 'Broker stopped. Session stats:'
    Write-Log "  Runtime:      $([int][math]::Floor($runtime.TotalHours)):$($runtime.ToString('mm\:ss'))"
    Write-Log "  Corruptions:  $($script:Stats.Corruptions)"
    Write-Log "  Restores:     $($script:Stats.Restores)"
    Write-Log "  Deep Merges:  $($script:Stats.Merges)"
    Write-Log "  Snapshots:    $($script:Stats.Snapshots)"
    Write-Log '==========================================================='
}
