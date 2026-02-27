/**
 * TransactionGates — BEGIN, COMMIT, ROLLBACK.
 * Uses Runner's snapshot/restore for isolation.
 */
import { Gate } from '../../core/Gate.js';
import { Event } from '../../core/Event.js';

export class TransactionManager {
  constructor() {
    this.snapshots = [];
    this.active = false;
  }

  isActive() { return this.active; }

  begin(snapshot) {
    if (this.active) throw new Error('Transaction already active');
    this.snapshots.push(snapshot);
    this.active = true;
  }

  commit() {
    if (!this.active) throw new Error('No active transaction');
    this.snapshots.pop();
    this.active = false;
  }

  rollback() {
    if (!this.active) throw new Error('No active transaction');
    this.active = false;
    return this.snapshots.pop();
  }
}

export class TransactionBeginGate extends Gate {
  constructor(txn, snapshotFn) {
    super('transaction_begin');
    this.txn = txn;
    this.snapshotFn = snapshotFn;
  }

  transform(event, stream) {
    try {
      const snapshot = this.snapshotFn();
      this.txn.begin(snapshot);
      stream.emit(new Event('transaction_begun', {}));
    } catch (err) {
      stream.emit(new Event('error', { message: err.message, source: 'transaction_begin' }));
    }
  }
}

export class TransactionCommitGate extends Gate {
  constructor(txn) {
    super('transaction_commit');
    this.txn = txn;
  }

  transform(event, stream) {
    try {
      this.txn.commit();
      stream.emit(new Event('transaction_committed', {}));
    } catch (err) {
      stream.emit(new Event('error', { message: err.message, source: 'transaction_commit' }));
    }
  }
}

export class TransactionRollbackGate extends Gate {
  constructor(txn, restoreFn) {
    super('transaction_rollback');
    this.txn = txn;
    this.restoreFn = restoreFn;
  }

  transform(event, stream) {
    try {
      const snapshot = this.txn.rollback();
      if (snapshot) this.restoreFn(snapshot);
      stream.emit(new Event('transaction_rolled_back', {}));
    } catch (err) {
      stream.emit(new Event('error', { message: err.message, source: 'transaction_rollback' }));
    }
  }
}
