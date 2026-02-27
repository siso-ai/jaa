import { test, assert, assertEqual, report } from './runner.js';
import { Runner } from '../src/resolution/Runner.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { MemoryRefs } from '../src/persistence/Refs.js';
import { registerDatabaseGates } from '../src/gates/database/register.js';
import { Event } from '../src/core/Event.js';

let store, refs, runner;

function fresh() {
  store = new MemoryStore();
  refs = new MemoryRefs();
  runner = new Runner({ store, refs });
  registerDatabaseGates(runner);

  runner.emit(new Event('create_table_execute', {
    table: 'users',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'text' },
      { name: 'age', type: 'integer' }
    ]
  }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice', age: 30 } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Bob', age: 25 } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Carol', age: 35 } }));
}

// ── index create ────────────────────────────────

test('Index: create builds index from existing rows', () => {
  fresh();
  runner.emit(new Event('index_create_execute', {
    table: 'users',
    index: 'age_idx',
    column: 'age'
  }));

  const idxHash = refs.get('db/tables/users/indexes/age_idx');
  assert(idxHash !== null, 'index ref exists');
  const idx = store.get(idxHash);
  assertEqual(idx.column, 'age');
  assert(idx.entries.length > 0, 'entries populated');

  const pending = runner.sample().pending;
  assert(pending.some(e => e.type === 'index_created'));
});

test('Index: create on nonexistent column errors', () => {
  fresh();
  runner.emit(new Event('index_create_execute', {
    table: 'users',
    index: 'bad_idx',
    column: 'nonexistent'
  }));
  assert(runner.sample().pending.some(e => e.type === 'error'));
});

// ── index scan ──────────────────────────────────

test('Index: scan with eq returns matching rows', () => {
  fresh();
  runner.emit(new Event('index_create_execute', {
    table: 'users', index: 'age_idx', column: 'age'
  }));

  runner.emit(new Event('index_scan', {
    table: 'users',
    index: 'age_idx',
    op: 'eq',
    value: 30
  }));

  const result = runner.sample().pending.find(e => e.type === 'scan_result');
  assert(result !== undefined);
  assertEqual(result.data.rows.length, 1);
  assertEqual(result.data.rows[0].name, 'Alice');
});

test('Index: scan with gte returns range', () => {
  fresh();
  runner.emit(new Event('index_create_execute', {
    table: 'users', index: 'age_idx', column: 'age'
  }));

  runner.emit(new Event('index_scan', {
    table: 'users',
    index: 'age_idx',
    op: 'gte',
    value: 30
  }));

  const result = runner.sample().pending.find(e => e.type === 'scan_result');
  assertEqual(result.data.rows.length, 2); // Alice(30), Carol(35)
});

// ── index drop ──────────────────────────────────

test('Index: drop removes index', () => {
  fresh();
  runner.emit(new Event('index_create_execute', {
    table: 'users', index: 'age_idx', column: 'age'
  }));
  runner.emit(new Event('index_drop_execute', {
    table: 'users', index: 'age_idx'
  }));
  assertEqual(refs.get('db/tables/users/indexes/age_idx'), null);
});

// ── insert updates existing index ───────────────

test('Index: insert updates index', () => {
  fresh();
  runner.emit(new Event('index_create_execute', {
    table: 'users', index: 'age_idx', column: 'age'
  }));

  runner.emit(new Event('insert_execute', {
    table: 'users', row: { name: 'Dave', age: 30 }
  }));

  // Scan for age=30 should now return 2 rows
  runner.emit(new Event('index_scan', {
    table: 'users', index: 'age_idx', op: 'eq', value: 30
  }));

  const result = runner.sample().pending.find(e => e.type === 'scan_result');
  assertEqual(result.data.rows.length, 2);
});

// ── view create ─────────────────────────────────

test('View: create stores view definition', () => {
  fresh();
  runner.emit(new Event('view_create_execute', {
    name: 'young_users',
    query: {
      pipeline: [
        { type: 'table_scan', data: { table: 'users' } },
        { type: 'filter', data: { where: { column: 'age', op: '<', value: 30 } } }
      ]
    }
  }));

  const viewHash = refs.get('db/views/young_users');
  assert(viewHash !== null, 'view ref exists');
  assertEqual(store.get(viewHash).name, 'young_users');

  assert(runner.sample().pending.some(e => e.type === 'view_created'));
});

test('View: create duplicate errors', () => {
  fresh();
  runner.emit(new Event('view_create_execute', { name: 'v', query: {} }));
  runner.emit(new Event('view_create_execute', { name: 'v', query: {} }));
  assert(runner.sample().pending.some(e => e.type === 'error'));
});

// ── view drop ───────────────────────────────────

test('View: drop removes view', () => {
  fresh();
  runner.emit(new Event('view_create_execute', { name: 'v', query: {} }));
  runner.emit(new Event('view_drop_execute', { name: 'v' }));
  assertEqual(refs.get('db/views/v'), null);
  assert(runner.sample().pending.some(e => e.type === 'view_dropped'));
});

// ── trigger create / drop ───────────────────────

test('Trigger: create and drop', () => {
  fresh();
  runner.emit(new Event('trigger_create_execute', {
    name: 'log_insert',
    table: 'users',
    timing: 'after',
    event: 'insert',
    action: 'log'
  }));

  const trigHash = refs.get('db/triggers/log_insert');
  assert(trigHash !== null);
  assertEqual(store.get(trigHash).table, 'users');

  runner.emit(new Event('trigger_drop_execute', { name: 'log_insert' }));
  assertEqual(refs.get('db/triggers/log_insert'), null);
});

test('Trigger: create duplicate errors', () => {
  fresh();
  runner.emit(new Event('trigger_create_execute', { name: 't', table: 'users', timing: 'after', event: 'insert', action: 'log' }));
  runner.emit(new Event('trigger_create_execute', { name: 't', table: 'users', timing: 'after', event: 'insert', action: 'log' }));
  assert(runner.sample().pending.some(e => e.type === 'error'));
});

// ── constraint create / drop ────────────────────

test('Constraint: create and drop', () => {
  fresh();
  runner.emit(new Event('constraint_create_execute', {
    table: 'users',
    name: 'age_positive',
    type: 'check',
    params: { column: 'age', op: '>', value: 0 }
  }));

  const cHash = refs.get('db/constraints/users/age_positive');
  assert(cHash !== null);
  assertEqual(store.get(cHash).type, 'check');

  runner.emit(new Event('constraint_drop_execute', {
    table: 'users', name: 'age_positive'
  }));
  assertEqual(refs.get('db/constraints/users/age_positive'), null);
});

test('Constraint: create on nonexistent table errors', () => {
  fresh();
  runner.emit(new Event('constraint_create_execute', {
    table: 'nope', name: 'c', type: 'check', params: {}
  }));
  assert(runner.sample().pending.some(e => e.type === 'error'));
});

const exitCode = report('meta-integration');
process.exit(exitCode);
