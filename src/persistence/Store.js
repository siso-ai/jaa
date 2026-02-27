/**
 * MemoryStore — content-addressable object store.
 *
 * put(content) → hash
 * get(hash) → content
 * has(hash) → boolean
 *
 * Content is serialized to canonical JSON, hashed with SHA-256.
 * Same content always returns the same hash (deduplication).
 * Objects are immutable once stored — get() returns a clone.
 * The store doesn't know what it's holding. A row, a schema,
 * a wiki page, a git blob — all the same.
 */
import { createHash } from 'node:crypto';
import { canonicalize } from './canonicalize.js';

export class MemoryStore {
  constructor() {
    this.objects = new Map(); // hash → canonical JSON string
  }

  /**
   * Store content, return its hash.
   * If the content already exists, no-op — returns same hash.
   */
  put(content) {
    const canonical = canonicalize(content);
    const hash = createHash('sha256').update(canonical).digest('hex');
    if (!this.objects.has(hash)) {
      this.objects.set(hash, canonical);
    }
    return hash;
  }

  /**
   * Retrieve content by hash.
   * Returns a deep clone — caller can't mutate stored data.
   * Throws if hash not found.
   */
  get(hash) {
    const canonical = this.objects.get(hash);
    if (canonical === undefined) {
      throw new Error(`Object not found: ${hash}`);
    }
    return JSON.parse(canonical);
  }

  /**
   * Check existence without deserializing.
   */
  has(hash) {
    return this.objects.has(hash);
  }
}
