/**
 * Recovery — write-ahead log for crash recovery.
 *
 * Before applying a MutationBatch, write intent to wal/pending.json.
 * After completing, delete pending.json.
 * On startup, if pending.json exists, replay unapplied operations.
 *
 * Store puts are idempotent (content-addressed), so replaying is safe.
 * Ref operations are idempotent (set overwrites, delete is no-op if missing).
 */
import { existsSync, mkdirSync, readFileSync, writeFileSync, unlinkSync } from 'fs';
import { join } from 'path';

export class Recovery {
  constructor(basePath) {
    this.basePath = basePath;
    this.walDir = join(basePath, 'wal');
    this.pendingPath = join(this.walDir, 'pending.json');
  }

  /**
   * Check for pending WAL entries.
   * Returns { clean: true } or { clean: false, pending: batch }.
   */
  check() {
    if (!existsSync(this.pendingPath)) {
      return { clean: true, pending: null };
    }
    const raw = readFileSync(this.pendingPath, 'utf8');
    const pending = JSON.parse(raw);
    return { clean: false, pending };
  }

  /**
   * Write a batch to the WAL before applying.
   * Returns the batch descriptor with applied flags.
   */
  begin(puts, refSets, refDeletes) {
    mkdirSync(this.walDir, { recursive: true });

    const batch = {
      timestamp: Date.now(),
      puts: puts.map(p => ({ hash: p.hash, content: p.content, applied: false })),
      refSets: refSets.map(r => ({ name: r.name, hash: r.hash, applied: false })),
      refDeletes: refDeletes.map(r => ({ name: r, applied: false })),
    };

    writeFileSync(this.pendingPath, JSON.stringify(batch, null, 2), 'utf8');
    return batch;
  }

  /**
   * Mark a put as applied in the WAL.
   */
  markPutApplied(batch, index) {
    batch.puts[index].applied = true;
    writeFileSync(this.pendingPath, JSON.stringify(batch, null, 2), 'utf8');
  }

  /**
   * Mark a refSet as applied in the WAL.
   */
  markRefSetApplied(batch, index) {
    batch.refSets[index].applied = true;
    writeFileSync(this.pendingPath, JSON.stringify(batch, null, 2), 'utf8');
  }

  /**
   * Mark a refDelete as applied in the WAL.
   */
  markRefDeleteApplied(batch, index) {
    batch.refDeletes[index].applied = true;
    writeFileSync(this.pendingPath, JSON.stringify(batch, null, 2), 'utf8');
  }

  /**
   * Complete the batch — delete the WAL entry.
   */
  commit() {
    if (existsSync(this.pendingPath)) {
      unlinkSync(this.pendingPath);
    }
  }

  /**
   * Recover from a crash — replay unapplied operations.
   * Requires store and refs instances to apply against.
   */
  recover(store, refs) {
    const status = this.check();
    if (status.clean) return;

    const batch = status.pending;

    // Replay unapplied puts
    for (const put of batch.puts) {
      if (!put.applied) {
        // Re-store the content. Content was serialized in the WAL.
        if (put.content !== undefined) {
          store.put(JSON.parse(put.content));
        }
      }
    }

    // Replay unapplied ref sets
    for (const refSet of batch.refSets) {
      if (!refSet.applied) {
        refs.set(refSet.name, refSet.hash);
      }
    }

    // Replay unapplied ref deletes
    for (const refDel of batch.refDeletes) {
      if (!refDel.applied) {
        refs.delete(refDel.name);
      }
    }

    this.commit();
  }
}
