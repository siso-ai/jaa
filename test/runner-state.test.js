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

// ── reads from persistence ──────────────────────

test('Runner+State: reads from persistence', () => {
  fresh();
  const hash = store.put({ name: 'users', columns: ['id', 'name'] });
  refs.set('db/tables/users/schema', hash);

  class ReadSchema extends StateGate {
    constructor() { super('read_schema'); }
    reads(event) {
      return new ReadSet().ref(`db/tables/${event.data.table}/schema`);
    }
    transform(event, state) {
      const schema = state.refs[`db/tables/${event.data.table}/schema`];
      return new MutationBatch()
        .emit(new Event('schema_result', { schema }));
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new ReadSchema());
  runner.emit(new Event('read_schema', { table: 'users' }));
  const result = runner.sample().pending[0];
  assertEqual(result.type, 'schema_result');
  assertEqual(result.data.schema.name, 'users');
  assertEqual(result.data.schema.columns[0], 'id');
});

// ── writes to persistence ───────────────────────

test('Runner+State: writes to persistence', () => {
  fresh();
  class CreateTable extends StateGate {
    constructor() { super('create_table'); }
    reads(event) { return new ReadSet(); }
    transform(event, state) {
      const schema = { name: event.data.table, columns: event.data.columns };
      return new MutationBatch()
        .put('schema', schema)
        .refSet(`db/tables/${event.data.table}/schema`, 0)
        .put('counter', '0')
        .refSet(`db/tables/${event.data.table}/next_id`, 1)
        .emit(new Event('table_created', { table: event.data.table }));
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new CreateTable());
  runner.emit(new Event('create_table', {
    table: 'users',
    columns: ['id', 'name']
  }));

  // Verify persistence was updated
  const schemaHash = refs.get('db/tables/users/schema');
  assert(schemaHash !== null, 'schema ref should exist');
  assertEqual(store.get(schemaHash).name, 'users');
  assertEqual(store.get(schemaHash).columns[0], 'id');

  const counterHash = refs.get('db/tables/users/next_id');
  assert(counterHash !== null, 'counter ref should exist');
  assertEqual(store.get(counterHash), '0');

  // Verify follow-up event
  assertEqual(runner.sample().pending[0].type, 'table_created');
  assertEqual(runner.sample().pending[0].data.table, 'users');
});

// ── missing ref resolved as null ────────────────

test('Runner+State: reads missing ref as null', () => {
  fresh();
  class CheckExists extends StateGate {
    constructor() { super('check'); }
    reads(event) { return new ReadSet().ref('db/tables/nope/schema'); }
    transform(event, state) {
      const exists = state.refs['db/tables/nope/schema'] !== null;
      return new MutationBatch()
        .emit(new Event('exists_result', { exists }));
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new CheckExists());
  runner.emit(new Event('check', {}));
  assertEqual(runner.sample().pending[0].data.exists, false);
});

// ── pattern reads ───────────────────────────────

test('Runner+State: pattern reads resolve all matching refs', () => {
  fresh();
  const h1 = store.put({ id: 1, name: 'Alice' });
  const h2 = store.put({ id: 2, name: 'Bob' });
  const h3 = store.put({ id: 3, name: 'Carol' });
  refs.set('db/tables/users/rows/1', h1);
  refs.set('db/tables/users/rows/2', h2);
  refs.set('db/tables/users/rows/3', h3);

  class ScanTable extends StateGate {
    constructor() { super('scan'); }
    reads(event) {
      return new ReadSet().pattern(`db/tables/${event.data.table}/rows/`);
    }
    transform(event, state) {
      const rows = Object.values(
        state.patterns[`db/tables/${event.data.table}/rows/`]
      );
      return new MutationBatch()
        .emit(new Event('scan_result', { rows }));
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new ScanTable());
  runner.emit(new Event('scan', { table: 'users' }));
  const result = runner.sample().pending[0];
  assertEqual(result.type, 'scan_result');
  assertEqual(result.data.rows.length, 3);
});

// ── empty pattern returns empty object ──────────

test('Runner+State: empty pattern returns empty object', () => {
  fresh();
  class ScanEmpty extends StateGate {
    constructor() { super('scan_empty'); }
    reads(event) {
      return new ReadSet().pattern('db/tables/empty/rows/');
    }
    transform(event, state) {
      const rows = Object.values(state.patterns['db/tables/empty/rows/']);
      return new MutationBatch()
        .emit(new Event('scan_result', { count: rows.length }));
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new ScanEmpty());
  runner.emit(new Event('scan_empty', {}));
  assertEqual(runner.sample().pending[0].data.count, 0);
});

// ── refSetHash with literal hash ────────────────

test('Runner+State: refSetHash with literal hash', () => {
  fresh();
  // Pre-store an object
  const existingHash = store.put({ old: true });

  class SwingRef extends StateGate {
    constructor() { super('swing'); }
    reads(event) { return new ReadSet(); }
    transform(event, state) {
      return new MutationBatch()
        .refSetHash('pointer', event.data.hash)
        .emit(new Event('swung', {}));
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new SwingRef());
  runner.emit(new Event('swing', { hash: existingHash }));
  assertEqual(refs.get('pointer'), existingHash);
  assertEqual(store.get(refs.get('pointer')).old, true);
});

// ── refDelete removes refs ──────────────────────

test('Runner+State: refDelete removes refs', () => {
  fresh();
  const h = store.put({ val: 1 });
  refs.set('db/tables/users/rows/1', h);
  refs.set('db/tables/users/rows/2', h);

  class DeleteRow extends StateGate {
    constructor() { super('delete_row'); }
    reads(event) { return new ReadSet(); }
    transform(event, state) {
      return new MutationBatch()
        .refDelete(`db/tables/users/rows/${event.data.id}`)
        .emit(new Event('deleted', { id: event.data.id }));
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new DeleteRow());
  runner.emit(new Event('delete_row', { id: 1 }));
  assertEqual(refs.get('db/tables/users/rows/1'), null);
  assert(refs.get('db/tables/users/rows/2') !== null, 'other row untouched');
  // Object still in store
  assertEqual(store.get(h).val, 1);
});

// ── events-only batch (no mutations) ────────────

test('Runner+State: events-only batch works', () => {
  fresh();
  const h = store.put({ id: 1, name: 'Alice' });
  refs.set('db/tables/users/rows/1', h);

  class Query extends StateGate {
    constructor() { super('query'); }
    reads(event) {
      return new ReadSet().pattern('db/tables/users/rows/');
    }
    transform(event, state) {
      const rows = Object.values(state.patterns['db/tables/users/rows/']);
      return new MutationBatch()
        .emit(new Event('query_result', { rows }));
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new Query());
  const sizeBefore = store.objects.size;
  runner.emit(new Event('query', {}));
  // No new objects stored
  assertEqual(store.objects.size, sizeBefore);
  // Result emitted
  assertEqual(runner.sample().pending[0].data.rows[0].name, 'Alice');
});

const exitCode = report('runner-state');
process.exit(exitCode);
