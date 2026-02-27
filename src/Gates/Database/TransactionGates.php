<?php
/**
 * TransactionGates — BEGIN, COMMIT, ROLLBACK.
 *
 * Uses Runner's snapshot/restore for isolation.
 * Stores a stack of snapshots to support (future) nested savepoints.
 */
namespace Ice\Gates\Database;

use Ice\Core\Gate;
use Ice\Core\Event;

class TransactionManager {
    /** @var array[] stack of snapshots */
    private array $snapshots = [];
    private bool $active = false;

    public function isActive(): bool { return $this->active; }

    public function begin(array $snapshot): void {
        if ($this->active) {
            throw new \RuntimeException('Transaction already active');
        }
        $this->snapshots[] = $snapshot;
        $this->active = true;
    }

    public function commit(): void {
        if (!$this->active) {
            throw new \RuntimeException('No active transaction');
        }
        array_pop($this->snapshots);
        $this->active = false;
    }

    public function rollback(): ?array {
        if (!$this->active) {
            throw new \RuntimeException('No active transaction');
        }
        $snapshot = array_pop($this->snapshots);
        $this->active = false;
        return $snapshot;
    }
}

/**
 * Begin gate — captures a snapshot as a savepoint.
 * Uses a shared TransactionManager via closure binding.
 */
class TransactionBeginGate extends Gate {
    private TransactionManager $txn;
    private \Closure $snapshotFn;

    public function __construct(TransactionManager $txn, \Closure $snapshotFn) {
        parent::__construct('transaction_begin');
        $this->txn = $txn;
        $this->snapshotFn = $snapshotFn;
    }

    public function process(Event $event): Event|array|null {
        try {
            $snapshot = ($this->snapshotFn)();
            $this->txn->begin($snapshot);
            return new Event('transaction_begun', []);
        } catch (\Throwable $e) {
            return new Event('error', ['message' => $e->getMessage(), 'source' => 'transaction_begin']);
        }
    }
}

/**
 * Commit gate — discards the savepoint, keeping current state.
 */
class TransactionCommitGate extends Gate {
    private TransactionManager $txn;

    public function __construct(TransactionManager $txn) {
        parent::__construct('transaction_commit');
        $this->txn = $txn;
    }

    public function process(Event $event): Event|array|null {
        try {
            $this->txn->commit();
            return new Event('transaction_committed', []);
        } catch (\Throwable $e) {
            return new Event('error', ['message' => $e->getMessage(), 'source' => 'transaction_commit']);
        }
    }
}

/**
 * Rollback gate — restores the snapshot, undoing all changes.
 */
class TransactionRollbackGate extends Gate {
    private TransactionManager $txn;
    private \Closure $restoreFn;

    public function __construct(TransactionManager $txn, \Closure $restoreFn) {
        parent::__construct('transaction_rollback');
        $this->txn = $txn;
        $this->restoreFn = $restoreFn;
    }

    public function process(Event $event): Event|array|null {
        try {
            $snapshot = $this->txn->rollback();
            if ($snapshot !== null) {
                ($this->restoreFn)($snapshot);
            }
            return new Event('transaction_rolled_back', []);
        } catch (\Throwable $e) {
            return new Event('error', ['message' => $e->getMessage(), 'source' => 'transaction_rollback']);
        }
    }
}
