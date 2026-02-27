/**
 * FileStore — file-based content-addressable store.
 *
 * Same interface as MemoryStore. put/get/has.
 * Objects stored as canonical JSON files, addressed by SHA-256 hash.
 * Directory layout mirrors git: store/{hash[0:2]}/{hash[2:]}.
 * Atomic writes via temp file + rename.
 */
import { createHash } from 'crypto';
import { existsSync, mkdirSync, readFileSync, writeFileSync, renameSync } from 'fs';
import { join, dirname } from 'path';
import { canonicalize } from './canonicalize.js';

export class FileStore {
  constructor({ basePath }) {
    this.root = join(basePath, 'store');
    mkdirSync(this.root, { recursive: true });
  }

  /**
   * Store content, return its hash.
   * Idempotent — same content always returns same hash, no duplicate files.
   */
  put(content) {
    const canonical = canonicalize(content);
    const hash = createHash('sha256').update(canonical).digest('hex');
    const path = this._path(hash);

    if (existsSync(path)) return hash; // dedup

    mkdirSync(dirname(path), { recursive: true });

    // Atomic write: write to temp, rename
    const tmp = path + '.tmp.' + process.pid;
    writeFileSync(tmp, canonical, 'utf8');
    renameSync(tmp, path);

    return hash;
  }

  /**
   * Retrieve content by hash.
   * Returns a fresh parsed clone. Throws if not found.
   */
  get(hash) {
    const path = this._path(hash);
    if (!existsSync(path)) {
      throw new Error(`Object not found: ${hash}`);
    }
    return JSON.parse(readFileSync(path, 'utf8'));
  }

  /**
   * Check if a hash exists in the store.
   */
  has(hash) {
    return existsSync(this._path(hash));
  }

  /** Internal: compute file path for a hash */
  _path(hash) {
    return join(this.root, hash.slice(0, 2), hash.slice(2));
  }
}
