import { test, assert, assertEqual, report } from './runner.js';
import { Runner } from '../src/resolution/Runner.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { MemoryRefs } from '../src/persistence/Refs.js';
import { StateGate } from '../src/protocol/StateGate.js';
import { ReadSet } from '../src/protocol/ReadSet.js';
import { MutationBatch } from '../src/protocol/MutationBatch.js';
import { Event } from '../src/core/Event.js';

let store, refs;

function fresh() {
  store = new MemoryStore();
  refs = new MemoryRefs();
}

// ── snapshot captures state ─────────────────────

test('Runner+Snapshot: captures current state', () => {
  fresh();
  const hash = store.put({ x: 1 });
  refs.set('key', hash);
  const runner = new Runner({ store, refs });
  const snap = runner.snapshot();
  assert(snap.store instanceof Map);
  assert(snap.refs instanceof Map);
  assertEqual(snap.store.size, 1);
  assertEqual(snap.refs.size, 1);
  assert(snap.refs.get('key') === hash);
});

// ── snapshot is a deep copy ─────────────────────

test('Runner+Snapshot: operations after snapshot do not affect it', () => {
  fresh();
  const hash = store.put({ original: true });
  refs.set('key', hash);
  const runner = new Runner({ store, refs });
  const snap = runner.snapshot();

  // Mutate after snapshot
  store.put({ after: true });
  refs.set('new_key', 'whatever');
  refs.set('key', 'changed');

  // Snapshot unchanged
  assertEqual(snap.store.size, 1);
  assertEqual(snap.refs.size, 1);
  assertEqual(snap.refs.get('key'), hash);
  assertEqual(snap.refs.has('new_key'), false);
});

// ── restore replaces state ──────────────────────

test('Runner+Snapshot: restore replaces current state', () => {
  fresh();
  const hash = store.put({ version: 1 });
  refs.set('data', hash);
  const runner = new Runner({ store, refs });
  const snap = runner.snapshot();

  // Mutate
  const hash2 = store.put({ version: 2 });
  refs.set('data', hash2);
  refs.set('extra', 'something');
  assertEqual(store.get(refs.get('data')).version, 2);

  // Restore
  runner.restore(snap);
  assertEqual(store.get(refs.get('data')).version, 1);
  assertEqual(refs.get('extra'), null);
});

// ── snapshot + restore for rollback ─────────────

test('Runner+Snapshot: rollback pattern', () => {
  fresh();

  class Increment extends StateGate {
    constructor() { super('increment'); }
    reads(event) { return new ReadSet().ref('counter'); }
    transform(event, state) {
      const val = state.refs['counter'] !== null ? state.refs['counter'] : 0;
      return new MutationBatch()
        .put('counter', val + 1)
        .refSet('counter', 0)
        .emit(new Event('incremented', { val: val + 1 }));
    }
  }

  const runner = new Runner({ store, refs });
  runner.register(new Increment());

  // Increment to 1
  runner.emit(new Event('increment', {}));
  assertEqual(store.get(refs.get('counter')), 1);

  // Snapshot at 1
  const snap = runner.snapshot();

  // Increment to 2 and 3
  runner.emit(new Event('increment', {}));
  runner.emit(new Event('increment', {}));
  assertEqual(store.get(refs.get('counter')), 3);

  // Rollback to 1
  runner.restore(snap);
  assertEqual(store.get(refs.get('counter')), 1);
});

// ── multiple snapshots ──────────────────────────

test('Runner+Snapshot: multiple snapshots are independent', () => {
  fresh();
  const runner = new Runner({ store, refs });

  store.put({ a: 1 });
  refs.set('count', store.put(10));
  const snap1 = runner.snapshot();

  refs.set('count', store.put(20));
  refs.set('extra', store.put('hello'));
  const snap2 = runner.snapshot();

  // snap1 has 1 ref, snap2 has 2
  assertEqual(snap1.refs.size, 1);
  assertEqual(snap2.refs.size, 2);

  // Restore snap1
  runner.restore(snap1);
  assertEqual(refs.get('extra'), null);
  assertEqual(store.get(refs.get('count')), 10);
});

const exitCode = report('runner-snapshot');
process.exit(exitCode);
