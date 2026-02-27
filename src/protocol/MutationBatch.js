/**
 * MutationBatch — describes what should change in persistence.
 *
 * A list of operations: puts, refSets, refDeletes, and
 * follow-up events. This is data, not execution. The
 * resolution layer (Phase 3) applies it atomically.
 *
 * Builder pattern — chain .put(), .refSet(), .refDelete(),
 * .emit() calls. The gate constructs this in transform().
 * It never calls store.put() or refs.set() directly.
 *
 * refSet() takes an index into the puts array. This avoids
 * the gate needing to compute hashes — the resolution layer
 * hashes the puts and resolves the indices.
 */
export class MutationBatch {
  constructor() {
    this.puts = [];       // { type, content } objects to store
    this.refSets = [];    // { name, putIndex?, hash? } refs to set
    this.refDeletes = []; // ref names to delete
    this.events = [];     // events to emit after mutations apply
  }

  /**
   * Add an object to store. Returns this for chaining.
   * The type is metadata for the caller — the store doesn't use it.
   */
  put(type, content) {
    this.puts.push({ type, content });
    return this;
  }

  /**
   * Set a ref to point at a put's hash (by index into puts array).
   * The resolution layer resolves the index after hashing the puts.
   */
  refSet(name, putIndex) {
    if (putIndex >= this.puts.length) {
      throw new Error(
        `refSet index ${putIndex} out of range (${this.puts.length} puts)`
      );
    }
    this.refSets.push({ name, putIndex });
    return this;
  }

  /**
   * Set a ref to a known hash directly.
   * Used when pointing to an object that already exists in the store.
   */
  refSetHash(name, hash) {
    this.refSets.push({ name, hash });
    return this;
  }

  /**
   * Delete a ref by name.
   */
  refDelete(name) {
    this.refDeletes.push(name);
    return this;
  }

  /**
   * Add a follow-up event to emit after mutations are applied.
   */
  emit(event) {
    this.events.push(event);
    return this;
  }
}
