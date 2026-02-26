# claude-config-broker

A PowerShell daemon that prevents `.claude.json` corruption caused by concurrent Claude Code sessions.

## The Problem

Claude Code stores its configuration in a single global file: `~/.claude.json`. When running multiple CLI sessions in parallel (a common power-user workflow), the file gets corrupted by a classic write-write race condition -- one process truncates the file while another is mid-write, resulting in partial JSON and `Unexpected EOF` errors.

```
Claude configuration file at C:\Users\<user>\.claude.json is corrupted: JSON Parse error: Unexpected EOF
The corrupted file has been backed up to: C:\Users\<user>\.claude\backups\.claude.json.corrupted.<timestamp>
```

This bug has been [reported at least 8 times since June 2025](https://github.com/anthropics/claude-code/issues/28922) and affects Windows, macOS, and Linux. As of v2.1.59, Claude Code still does not use atomic writes or file locking.

With just 2 parallel sessions, users report **hundreds of corrupted backup files per day**. The corruption is non-destructive (Claude Code recovers from backups), but disruptive -- error messages spam every open terminal and interrupt workflow.

## How It Works

The Config Broker Daemon acts as a post-write serializer that sits between Claude Code's writes and the filesystem:

```
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
```

On every detected file change:

1. **Validate** -- Parse the JSON. If valid, proceed. If corrupt, restore immediately.
2. **Lock** -- Acquire a system-wide Named Mutex (`Global\ClaudeConfigBrokerMutex`) to serialize all operations.
3. **Merge** -- Deep-merge the incoming write with the shadow state (kept in RAM) to preserve changes from all sessions.
4. **Write** -- Write the merged result back using atomic temp-file + rename.

Corruption is typically repaired within ~300-400ms, before the next Claude session reads the file.

## Features

- **Event-based FileSystemWatcher** with debounce -- no polling, low CPU usage
- **Gap detection fallback** -- catches the rare missed FSW event via `LastWriteTimeUtc` comparison
- **Named Mutex** -- system-wide cross-process locking (no admin required)
- **JSON deep-merge** -- concurrent changes from different sessions are preserved, not overwritten
- **Atomic writes** -- temp-file + rename pattern prevents further corruption
- **Shadow state** -- persisted to RAM and disk for crash recovery
- **Cross-runtime** -- works on PowerShell 5.1 (.NET Framework) and PowerShell 7+ (.NET 6/8/9)
- **Single-instance guard** -- only one broker runs per machine, safe to call from multiple terminals
- **Log rotation** -- automatic rotation at configurable size
- **Stale backup cleanup** -- removes accumulated `.corrupted.*` files older than N days

## Requirements

- Windows (tested on Windows 10/11)
- PowerShell 5.1 or later (ships with Windows)
- Claude Code (any version affected by the race condition)

## Installation

1. Download `Invoke-ClaudeConfigBroker.ps1` to a permanent location:

```powershell
# Example: save to your user scripts folder
mkdir -Force "$env:USERPROFILE\Scripts"
Copy-Item Invoke-ClaudeConfigBroker.ps1 "$env:USERPROFILE\Scripts\"
```

2. (Optional) Add auto-start to your PowerShell profile so the broker launches with your first terminal:

```powershell
# Open your profile
code $PROFILE
# or: notepad $PROFILE

# Add this block:
$brokerScript = "$env:USERPROFILE\Scripts\Invoke-ClaudeConfigBroker.ps1"
if (Test-Path $brokerScript) {
    Start-Job -FilePath $brokerScript -Name 'ClaudeBroker' | Out-Null
}
```

The single-instance guard ensures only one broker runs, no matter how many terminals you open.

## Usage

### Interactive (recommended for first run)

Run in a separate terminal to see live output:

```powershell
.\Invoke-ClaudeConfigBroker.ps1
```

You'll see:

```
  +---------------------------------------------------------+
  |        Claude Config Broker Daemon v1.1                 |
  |        "Reverse JSON Proxy" for .claude.json            |
  +---------------------------------------------------------+
    Mutex:     Global\ClaudeConfigBrokerMutex
    Config:    C:\Users\you\.claude.json
    Shadow:    C:\Users\you\.claude\backups\.claude.json.broker-shadow
    Strategy:  DeepMerge
    Debounce:  300ms
    GapCheck:  2000ms
    LogMax:    10 MB
  +---------------------------------------------------------+

[2026-02-26 16:40:21.401] [INFO] Initializing...
[2026-02-26 16:40:21.422] [INFO] Initial config loaded into shadow state.
[2026-02-26 16:40:21.456] [INFO] Broker is active. Press Ctrl+C to stop.
```

Then start your Claude Code sessions in other terminals as usual.

### Background job

```powershell
$broker = Start-Job -FilePath .\Invoke-ClaudeConfigBroker.ps1

# Check status
Receive-Job $broker

# Stop
Stop-Job $broker
```

### Parameters

| Parameter | Default | Description |
|---|---|---|
| `-MergeStrategy` | `DeepMerge` | `DeepMerge` preserves changes from all sessions. `LastValidWins` is simpler -- no merge, just keeps the last valid state. |
| `-DebounceMs` | `300` | Wait time after last file event before processing. Increase to `500` if running 7+ parallel sessions. |
| `-GapCheckMs` | `2000` | Fallback interval to detect missed FSW events. |
| `-MutexTimeoutMs` | `5000` | Max wait to acquire the named mutex. |
| `-CleanupDays` | `7` | Delete `.corrupted.*` backup files older than this. |
| `-LogMaxSizeMB` | `10` | Log file rotation threshold. |
| `-LogFile` | `~/.claude/broker.log` | Log file path. |

### Examples

```powershell
# Conservative mode -- no merging, pure corruption protection
.\Invoke-ClaudeConfigBroker.ps1 -MergeStrategy LastValidWins

# Tuned for many parallel sessions
.\Invoke-ClaudeConfigBroker.ps1 -DebounceMs 500

# Faster gap detection + smaller log
.\Invoke-ClaudeConfigBroker.ps1 -GapCheckMs 1000 -LogMaxSizeMB 5
```

## Merge Strategies

### DeepMerge (default)

Recursively merges the incoming write with the shadow state:

- **Scalar values**: incoming wins
- **Nested objects**: recursively merged (both sides preserved)
- **Arrays**: incoming wins (complete replacement -- Claude config arrays represent full state)
- **Properties only in shadow**: preserved (with a warning logged)

**Limitation**: When Claude CLI intentionally deletes a JSON property, DeepMerge will restore it from the shadow state because no common ancestor is available (two-way merge). If this causes issues, switch to `LastValidWins`.

### LastValidWins

No merging. On valid writes, the shadow state is simply replaced. On corruption, the last valid state is restored. Simpler, safer against the deletion limitation, but concurrent changes from different sessions may be lost.

## How It Recovers From Corruption

When a corrupt `.claude.json` is detected:

1. **Shadow state (RAM)** -- fastest, always tried first
2. **Shadow state (disk)** -- survives broker restarts and crashes
3. **Claude Code backups** -- searches `~/.claude/backups/.claude.json.backup.*` (newest first, validated before use)

## Files Created

| File | Purpose |
|---|---|
| `~/.claude/broker.log` | Daemon log (rotated) |
| `~/.claude/broker.pid` | PID file for external tooling |
| `~/.claude/backups/.claude.json.broker-shadow` | Persisted shadow state |

## No Admin Required

The broker runs entirely in user-space. The `Global\` prefix on the Named Mutex means "cross-session" (visible across all terminal windows), not "requires elevation".

## FAQ

**Does this fix the root cause?**
No. The root cause is that Claude Code writes to `~/.claude.json` without atomic operations or file locking. This daemon is a workaround that heals the corruption reactively. The real fix needs to come from Anthropic -- see [#28922](https://github.com/anthropics/claude-code/issues/28922).

**Is there a performance impact?**
Negligible. The daemon is event-driven (not polling), uses minimal CPU while idle, and only wakes up when `.claude.json` actually changes.

**Can this make things worse?**
The broker uses atomic writes (temp + rename) for all its own operations and validates JSON before writing. It cannot introduce corruption. In the worst case (broker crash, mutex timeout), it simply does nothing and Claude Code's built-in backup recovery takes over.

**Does it work on macOS/Linux?**
Currently Windows-only. The Named Mutex API is Windows-specific. A cross-platform version using `flock` would be possible but is not implemented. Contributions welcome.

## Related Issues

- [#28922](https://github.com/anthropics/claude-code/issues/28922) -- Meta-issue: reported 8 times since June 2025
- [#28847](https://github.com/anthropics/claude-code/issues/28847) -- Race condition with multiple instances
- [#28813](https://github.com/anthropics/claude-code/issues/28813) -- Concurrent CLI sessions on Windows
- [#3117](https://github.com/anthropics/claude-code/issues/3117) -- Same issue on macOS (July 2025)

## License

MIT
