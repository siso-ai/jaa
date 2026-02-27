/**
 * MemoryRefs — named references to store hashes.
 *
 * set(name, hash)
 * get(name) → hash | null
 * delete(name)
 * list(prefix) → [names]
 *
 * A flat map of strings to strings. The ref layer doesn't
 * know what the hashes point to. It doesn't validate that
 * a hash exists in the store. That's the caller's concern.
 */
export class MemoryRefs {
  constructor() {
    this.names = new Map(); // name → hash
  }

  /**
   * Point a name at a hash. Creates or overwrites.
   */
  set(name, hash) {
    this.names.set(name, hash);
  }

  /**
   * Look up a name. Returns the hash, or null if not found.
   * Null, not throw — absence is normal for refs.
   */
  get(name) {
    return this.names.get(name) ?? null;
  }

  /**
   * Remove a name. No-op if it doesn't exist.
   */
  delete(name) {
    this.names.delete(name);
  }

  /**
   * List all names starting with prefix, sorted.
   * Empty prefix returns all names.
   */
  list(prefix) {
    const matches = [];
    for (const key of this.names.keys()) {
      if (key.startsWith(prefix)) {
        matches.push(key);
      }
    }
    return matches.sort();
  }
}
