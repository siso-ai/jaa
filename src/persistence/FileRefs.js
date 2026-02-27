/**
 * FileRefs — file-based ref map.
 *
 * Same interface as MemoryRefs. set/get/delete/list.
 * Ref names map directly to file paths: refs/{name}.
 * Directory structure IS the namespace hierarchy.
 * Atomic writes via temp file + rename.
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync, renameSync, unlinkSync, readdirSync, statSync, rmdirSync } from 'fs';
import { join, dirname, relative } from 'path';

export class FileRefs {
  constructor({ basePath }) {
    this.root = join(basePath, 'refs');
    mkdirSync(this.root, { recursive: true });
  }

  /**
   * Set a ref name to a hash.
   * Creates directory structure as needed.
   */
  set(name, hash) {
    const path = this._path(name);
    mkdirSync(dirname(path), { recursive: true });

    // Atomic write
    const tmp = path + '.tmp.' + process.pid;
    writeFileSync(tmp, hash, 'utf8');
    renameSync(tmp, path);
  }

  /**
   * Get the hash for a ref name.
   * Returns null if the ref doesn't exist.
   */
  get(name) {
    const path = this._path(name);
    if (!existsSync(path)) return null;
    return readFileSync(path, 'utf8').trim();
  }

  /**
   * Delete a ref.
   * Cleans up empty parent directories.
   */
  delete(name) {
    const path = this._path(name);
    if (!existsSync(path)) return;
    unlinkSync(path);
    this._cleanEmptyDirs(dirname(path));
  }

  /**
   * List all ref names that start with prefix.
   * Returns sorted array of full ref names.
   */
  list(prefix) {
    const searchDir = join(this.root, prefix);
    if (!existsSync(searchDir)) {
      // prefix might point to a partial directory name
      // Try the parent directory and filter
      const parent = dirname(searchDir);
      const partial = searchDir.slice(parent.length + 1);
      if (!existsSync(parent)) return [];
      return this._walk(parent)
        .map(f => relative(this.root, f))
        .filter(name => name.startsWith(prefix))
        .sort();
    }

    const stat = statSync(searchDir);
    if (stat.isFile()) {
      // Exact match — prefix is a full ref name
      return [prefix];
    }

    // Walk directory
    return this._walk(searchDir)
      .map(f => relative(this.root, f))
      .sort();
  }

  /** Internal: compute file path for a name */
  _path(name) {
    return join(this.root, name);
  }

  /** Internal: recursively walk directory, return all file paths */
  _walk(dir) {
    const results = [];
    if (!existsSync(dir)) return results;

    const entries = readdirSync(dir);
    for (const entry of entries) {
      // Skip tmp files
      if (entry.includes('.tmp.')) continue;
      const full = join(dir, entry);
      const stat = statSync(full);
      if (stat.isDirectory()) {
        results.push(...this._walk(full));
      } else {
        results.push(full);
      }
    }
    return results;
  }

  /** Internal: remove empty parent directories up to root */
  _cleanEmptyDirs(dir) {
    while (dir !== this.root && dir.startsWith(this.root)) {
      try {
        const entries = readdirSync(dir);
        if (entries.length === 0) {
          rmdirSync(dir);
          dir = dirname(dir);
        } else {
          break;
        }
      } catch {
        break;
      }
    }
  }
}
