/**
 * Runner — the resolution layer.
 *
 * The impure shell. Owns a Stream, a Store, and a Refs.
 * The only thing in the system that touches all three.
 *
 * When a PureGate is registered, the Runner wraps it:
 *   event → gate.transform(event) → emit result
 *
 * When a StateGate is registered, the Runner wraps it:
 *   event → gate.reads(event) → resolve against persistence
 *   → gate.transform(event, state) → apply mutations → emit follow-ups
 *
 * The Stream is unchanged. It sees plain Gates with transform().
 * The gates are unchanged. They see plain events and state objects.
 * The Runner is the quarantine boundary between purity and the world.
 */
import { Stream } from '../core/Stream.js';
import { Gate } from '../core/Gate.js';
import { Event } from '../core/Event.js';
import { PureGate } from '../protocol/PureGate.js';
import { StateGate } from '../protocol/StateGate.js';

export class Runner {
  constructor({ store, refs, log = null }) {
    this.store = store;
    this.refs = refs;
    this.log = log;
    this.stream = new Stream({ log });
  }

  /**
   * Register a gate. Wraps it in a resolution shell so the
   * Stream sees a plain Gate. The wrapper dispatches based on
   * whether the gate is a PureGate or StateGate.
   */
  register(gate) {
    const runner = this;
    const wrapper = new Gate(gate.signature);

    if (gate instanceof StateGate) {
      wrapper.transform = function (event, stream) {
        try {
          const readSet = gate.reads(event);
          const state = runner.resolve(readSet);
          const batch = gate.transform(event, state);
          runner.apply(batch, stream);
        } catch (err) {
          stream.emit(new Event('error', {
            message: err.message,
            source: gate.signature,
          }));
        }
      };
    } else if (gate instanceof PureGate) {
      wrapper.transform = function (event, stream) {
        try {
          const result = gate.transform(event);
          if (Array.isArray(result)) {
            for (const r of result) stream.emit(r);
          } else if (result) {
            stream.emit(result);
          }
        } catch (err) {
          stream.emit(new Event('error', {
            message: err.message,
            source: gate.signature,
          }));
        }
      };
    } else {
      // Plain Gate — register as-is (backward compatible)
      this.stream.register(gate);
      return;
    }

    this.stream.register(wrapper);
  }

  /**
   * Emit an event into the stream.
   * Depth-first processing, same as always.
   */
  emit(event) {
    this.stream.emit(event);
  }

  /**
   * Observe the current state of the stream.
   * Same philosophy as Stream.sampleHere().
   */
  sample() {
    return this.stream.sampleHere();
  }

  /**
   * Resolve a ReadSet against persistence.
   * Returns a plain state object the gate can read from.
   *
   * This is the only place that calls store.get() and refs.get()
   * for gate reads. All reads are quarantined here.
   */
  resolve(readSet) {
    const state = { refs: {}, patterns: {} };

    // Resolve specific refs
    for (const name of readSet.refs) {
      const hash = this.refs.get(name);
      if (hash === null) {
        state.refs[name] = null;
      } else {
        state.refs[name] = this.store.get(hash);
      }
    }

    // Resolve prefix patterns
    for (const pattern of readSet.patterns) {
      state.patterns[pattern] = {};
      const names = this.refs.list(pattern);
      for (const name of names) {
        const hash = this.refs.get(name);
        state.patterns[pattern][name] = this.store.get(hash);
      }
    }

    return state;
  }

  /**
   * Apply a MutationBatch to persistence.
   * Puts first (safe, idempotent), then ref operations, then events.
   *
   * This is the only place that calls store.put() and refs.set()
   * for gate writes. All writes are quarantined here.
   */
  apply(batch, stream) {
    // 1. Store all puts, collect hashes
    const hashes = [];
    for (const put of batch.puts) {
      const hash = this.store.put(put.content);
      hashes.push(hash);
    }

    // 2. Apply ref sets
    for (const refSet of batch.refSets) {
      const hash = refSet.putIndex !== undefined
        ? hashes[refSet.putIndex]
        : refSet.hash;
      this.refs.set(refSet.name, hash);
    }

    // 3. Apply ref deletes
    for (const name of batch.refDeletes) {
      this.refs.delete(name);
    }

    // 4. Emit follow-up events (continues depth-first)
    for (const event of batch.events) {
      stream.emit(event);
    }
  }

  /**
   * Deep copy of current persistence state.
   * For testing and debugging.
   */
  snapshot() {
    const storeSnapshot = new Map();
    for (const [hash, canonical] of this.store.objects) {
      storeSnapshot.set(hash, canonical);
    }

    const refsSnapshot = new Map();
    for (const [name, hash] of this.refs.names) {
      refsSnapshot.set(name, hash);
    }

    return { store: storeSnapshot, refs: refsSnapshot };
  }

  /**
   * Restore persistence state from a snapshot.
   * For rollback testing.
   */
  restore(snapshot) {
    this.store.objects.clear();
    for (const [hash, canonical] of snapshot.store) {
      this.store.objects.set(hash, canonical);
    }

    this.refs.names.clear();
    for (const [name, hash] of snapshot.refs) {
      this.refs.names.set(name, hash);
    }
  }

  /** Clear pending events (for REPL use). */
  clearPending() {
    this.stream.pending = [];
  }
}
