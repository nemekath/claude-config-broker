#Requires -Version 5.1
<#
.SYNOPSIS
    Config Broker Daemon v1.1 -- a "reverse JSON proxy" that serializes concurrent
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
    Milliseconds to wait after the last file event before processing (default: 300).
    Prevents multiple processing runs during rapid successive writes.

.PARAMETER GapCheckMs
    Timeout in milliseconds for gap detection fallback (default: 2000).
    When no FSW event arrives within this window, the daemon checks the file's
    LastWriteTimeUtc to catch any missed changes. Lower values detect gaps faster
    but increase idle CPU usage slightly.

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
    [int]$DebounceMs      = 300,

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
Get-ChildItem -Path $WatchDir -Filter '.claude.json.broker-tmp.*' -ErrorAction SilentlyContinue |
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

# Write PID file for external tools
$PID | Out-File -FilePath $PidFile -Encoding ascii -Force

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
    } catch { }
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
    try { $entry | Out-File -FilePath $LogFile -Append -Encoding ascii } catch { }
}

# =============================================================================
# NAMED MUTEX WRAPPER
# =============================================================================
$configMutex = [System.Threading.Mutex]::new($false, $MutexName)

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
        Write-Log "Lock acquired: $Description" 'LOCK'
        return & $Action
    } catch [System.Threading.AbandonedMutexException] {
        # Another process holding the mutex crashed -- we inherit it
        $acquired = $true
        Write-Log "Inherited abandoned mutex for: $Description" 'WARN'
        return & $Action
    } finally {
        if ($acquired) {
            try { $configMutex.ReleaseMutex() } catch { }
            Write-Log "Lock released: $Description" 'LOCK'
        }
    }
}

# =============================================================================
# CROSS-RUNTIME FILE MOVE (PS 5.1 + PS 7)
# =============================================================================
# .NET 6+ has File.Move(src, dst, overwrite). .NET Framework does not.
# Detect once at startup, then use the fast path or fallback.
$script:HasMoveOverwrite = @([System.IO.File].GetMethods() |
    Where-Object { $_.Name -eq 'Move' -and $_.GetParameters().Count -eq 3 }).Count -gt 0

function Move-FileAtomic {
    <#
    .SYNOPSIS
        Moves src to dst, overwriting dst if it exists.
        Uses File.Move(s,d,$true) on .NET 6+, Delete+Move on .NET Framework.
    #>
    param([string]$Source, [string]$Destination)
    if ($script:HasMoveOverwrite) {
        [System.IO.File]::Move($Source, $Destination, $true)
    } else {
        # .NET Framework fallback: delete then move (tiny race window, acceptable for local config)
        if ([System.IO.File]::Exists($Destination)) {
            [System.IO.File]::Delete($Destination)
        }
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

        [System.IO.File]::WriteAllText($tempPath, $Json, [System.Text.Encoding]::UTF8)

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
            return @($Obj | ForEach-Object { Sort-Properties $_ })
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
$script:ShadowTimestamp = [datetime]::MinValue
$script:LastPersistedShadowJson = ''

function Update-ShadowState {
    param([object]$State, [string]$Json = $null)
    $script:ShadowState = $State
    $script:ShadowTimestamp = Get-Date

    if (-not $Json) { $Json = $State | ConvertTo-Json -Depth 50 }

    # Skip disk persist if content is identical to last persisted shadow
    if ($Json -eq $script:LastPersistedShadowJson) { return }

    # Persist shadow to disk atomically (non-critical, best-effort)
    try {
        $shadowTmp = "$ShadowFile.tmp.$PID"
        [System.IO.File]::WriteAllText($shadowTmp, $Json, [System.Text.Encoding]::UTF8)
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
                    Write-Log "DeepMerge: $baseOnlyCount shadow-only property/ies preserved (may be intentional deletions). Use -MergeStrategy LastValidWins to avoid." 'WARN'
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
                Sort-Object LastWriteTime -Descending)

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
    $cutoff = (Get-Date).AddDays(-$CleanupDays)
    [array]$stale = @(Get-ChildItem -Path $BackupDir -Filter '.claude.json.corrupted.*' -ErrorAction SilentlyContinue |
        Where-Object { $_.LastWriteTime -lt $cutoff })
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
  |        Claude Config Broker Daemon v1.1                 |
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
    StartTime   = Get-Date
}

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
try {
    while ($true) {
        # Idle wait: block until an event arrives or gap-check timeout
        $gapTimeoutSec = [math]::Max(1, [math]::Ceiling($GapCheckMs / 1000))
        $ev = Wait-Event -Timeout $gapTimeoutSec

        if ($ev) {
            # Event received — drain all queued events, then debounce
            @($ev) | ForEach-Object { Remove-Event -EventIdentifier $_.EventIdentifier }
            Get-Event -ErrorAction SilentlyContinue | Remove-Event

            # Debounce: wait for writes to settle (no new events for DebounceMs)
            $debounceEnd = (Get-Date).AddMilliseconds($DebounceMs)
            while ((Get-Date) -lt $debounceEnd) {
                Start-Sleep -Milliseconds 50
                $pending = @(Get-Event -ErrorAction SilentlyContinue)
                if ($pending.Count -gt 0) {
                    $pending | Remove-Event
                    # New events arrived — reset debounce window
                    $debounceEnd = (Get-Date).AddMilliseconds($DebounceMs)
                }
            }

            # Debounce settled — process
            Invoke-ProcessConfigChange
            $script:LastKnownWriteTime = ([System.IO.FileInfo]::new($ConfigFile)).LastWriteTimeUtc
        } else {
            # Timeout — gap detection fallback (covers events missed during processing)
            $currentWriteTime = ([System.IO.FileInfo]::new($ConfigFile)).LastWriteTimeUtc
            if ($currentWriteTime -gt $script:LastKnownWriteTime) {
                Write-Log 'Detected missed file change (watcher gap recovery).' 'WARN'
                Invoke-ProcessConfigChange
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
    try { $configMutex.ReleaseMutex() } catch { }
    $configMutex.Dispose()

    # Remove PID file
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue

    if ($null -ne $instanceMutex) {
        try { $instanceMutex.ReleaseMutex() } catch { }
        $instanceMutex.Dispose()
    }

    $runtime = (Get-Date) - $script:Stats.StartTime
    Write-Host ''
    Write-Log '==========================================================='
    Write-Log 'Broker stopped. Session stats:'
    Write-Log "  Runtime:      $($runtime.ToString('hh\:mm\:ss'))"
    Write-Log "  Corruptions:  $($script:Stats.Corruptions)"
    Write-Log "  Restores:     $($script:Stats.Restores)"
    Write-Log "  Deep Merges:  $($script:Stats.Merges)"
    Write-Log "  Snapshots:    $($script:Stats.Snapshots)"
    Write-Log '==========================================================='
}
