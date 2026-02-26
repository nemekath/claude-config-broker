/**
 * claude-atomic-writes.js — Bun preload module for atomic .claude.json writes
 *
 * Prevents concurrent-write corruption by intercepting file writes that target
 * .claude.json and replacing them with atomic temp-file + rename operations.
 *
 * Usage: Place in ~/bunfig.toml as a preload:
 *   [run]
 *   preload = ["~/.claude/claude-atomic-writes.js"]
 *
 * Compatible with: Bun standalone executables (claude.exe), Node.js --require
 */

'use strict';

const fs = require('fs');
const path = require('path');
const os = require('os');
const { URL: FileURL } = require('url');

const TARGET_BASENAME = '.claude.json';
const TEMP_PREFIX = '.broker-atomic.';
// Normalize home dir to lowercase for case-insensitive comparison on Windows
const HOME_DIR = os.homedir();
const HOME_DIR_LOWER = HOME_DIR.toLowerCase();

// Lightweight uniqueness counter (avoids loading crypto module at startup)
let _seq = 0;

/**
 * Normalize a file argument to a string path.
 * Handles: string, Buffer, URL (file:// protocol).
 */
function toFilePath(file) {
  if (typeof file === 'string') return file;
  if ((FileURL && file instanceof FileURL) || (file && typeof file === 'object' && file.protocol === 'file:')) {
    try { return require('url').fileURLToPath(file); } catch (_e) { return null; }
  }
  if (Buffer.isBuffer(file)) return file.toString();
  return null;
}

/**
 * Check if a file path targets .claude.json in the user's home directory.
 */
function isClaudeConfig(filePath) {
  if (!filePath || typeof filePath !== 'string') return false;
  try {
    const resolved = path.resolve(filePath);
    if (path.basename(resolved) !== TARGET_BASENAME) return false;
    // Case-insensitive directory comparison (Windows paths are case-insensitive)
    return path.dirname(resolved).toLowerCase() === HOME_DIR_LOWER;
  } catch (_e) {
    return false;
  }
}

/**
 * Generate a unique temp file path for atomic writes.
 * Uses PID + monotonic counter for uniqueness (no crypto dependency).
 */
function tempPath(targetPath) {
  return `${targetPath}${TEMP_PREFIX}${process.pid}.${++_seq}`;
}

/**
 * Clean up a temp file, ignoring errors.
 */
function cleanupSync(tmpFile) {
  try { fs.unlinkSync(tmpFile); } catch (_e) { /* ignore */ }
}

/**
 * Rename with single retry on EACCES/EPERM (Windows sharing violation).
 * Antivirus, Search Indexer, or backup software may briefly hold a handle.
 */
function renameSyncRetry(src, dest) {
  try {
    fs.renameSync(src, dest);
  } catch (err) {
    if (err.code === 'EACCES' || err.code === 'EPERM') {
      // Busy-wait ~50ms then retry once (handle typically released within ms)
      const start = Date.now();
      while (Date.now() - start < 50) { /* spin */ }
      fs.renameSync(src, dest);
    } else {
      throw err;
    }
  }
}

// ---------------------------------------------------------------------------
// Patch fs.writeFileSync
// ---------------------------------------------------------------------------
const origWriteFileSync = fs.writeFileSync;

fs.writeFileSync = function patchedWriteFileSync(file, data, options) {
  const filePath = toFilePath(file);
  if (!isClaudeConfig(filePath)) {
    return origWriteFileSync.call(fs, file, data, options);
  }

  const resolved = path.resolve(filePath);
  const tmp = tempPath(resolved);
  try {
    origWriteFileSync.call(fs, tmp, data, options);
    renameSyncRetry(tmp, resolved);
  } catch (err) {
    cleanupSync(tmp);
    throw err;
  }
};

// ---------------------------------------------------------------------------
// Patch fs.writeFile (async callback version)
// ---------------------------------------------------------------------------
const origWriteFile = fs.writeFile;

fs.writeFile = function patchedWriteFile(file, data, optionsOrCb, maybeCb) {
  // Normalize arguments: writeFile(path, data, cb) or writeFile(path, data, options, cb)
  let options, callback;
  if (typeof optionsOrCb === 'function') {
    callback = optionsOrCb;
    options = undefined;
  } else {
    options = optionsOrCb;
    callback = maybeCb;
  }

  const filePath = toFilePath(file);
  if (!isClaudeConfig(filePath)) {
    if (options !== undefined) {
      return origWriteFile.call(fs, file, data, options, callback);
    }
    return origWriteFile.call(fs, file, data, callback);
  }

  const resolved = path.resolve(filePath);
  const tmp = tempPath(resolved);

  // Ensure callback exists — matches Node.js behavior for missing callback
  // (throws on next tick rather than inside libuv's async context)
  if (typeof callback !== 'function') {
    callback = (err) => { if (err) process.nextTick(() => { throw err; }); };
  }

  const writeCb = (err) => {
    if (err) {
      cleanupSync(tmp);
      callback(err);
      return;
    }
    try {
      renameSyncRetry(tmp, resolved);
      callback(null);
    } catch (renameErr) {
      cleanupSync(tmp);
      callback(renameErr);
    }
  };

  if (options !== undefined) {
    origWriteFile.call(fs, tmp, data, options, writeCb);
  } else {
    origWriteFile.call(fs, tmp, data, writeCb);
  }
};

// ---------------------------------------------------------------------------
// Patch fs.promises.writeFile
// ---------------------------------------------------------------------------
const origPromisesWriteFile = fs.promises.writeFile;

fs.promises.writeFile = async function patchedPromisesWriteFile(file, data, options) {
  const filePath = toFilePath(file);
  if (!isClaudeConfig(filePath)) {
    return origPromisesWriteFile.call(fs.promises, file, data, options);
  }

  const resolved = path.resolve(filePath);
  const tmp = tempPath(resolved);
  try {
    await origPromisesWriteFile.call(fs.promises, tmp, data, options);
    // Sync rename: same-directory rename is a metadata-only operation on NTFS/ext4,
    // effectively atomic for same-volume paths (which is always the case here).
    renameSyncRetry(tmp, resolved);
  } catch (err) {
    cleanupSync(tmp);
    throw err;
  }
};

// NOTE: fs.createWriteStream is not patched. JSON configs are not written via
// streams in any known Claude Code version. Layer 2 (broker daemon) covers this.

// ---------------------------------------------------------------------------
// Patch Bun.write (if running under Bun)
// ---------------------------------------------------------------------------
if (typeof globalThis.Bun !== 'undefined' && typeof globalThis.Bun.write === 'function') {
  const origBunWrite = globalThis.Bun.write.bind(globalThis.Bun);

  globalThis.Bun.write = function patchedBunWrite(destination, data, options) {
    // Bun.write(path, data, options?) — destination can be a string path or a Bun file descriptor
    let filePath = null;
    if (typeof destination === 'string') {
      filePath = destination;
    } else if (destination && typeof destination === 'object' && typeof destination.name === 'string') {
      // Bun.file() returns an object with .name
      filePath = destination.name;
    }

    if (!filePath || !isClaudeConfig(filePath)) {
      return options !== undefined ? origBunWrite(destination, data, options) : origBunWrite(destination, data);
    }

    const resolved = path.resolve(filePath);
    const tmp = tempPath(resolved);

    // Bun.write returns a Promise<number> (bytes written) — preserve the return value
    const writePromise = options !== undefined ? origBunWrite(tmp, data, options) : origBunWrite(tmp, data);
    return writePromise.then((bytesWritten) => {
      renameSyncRetry(tmp, resolved);
      return bytesWritten;
    }).catch((err) => {
      cleanupSync(tmp);
      throw err;
    });
  };
}
