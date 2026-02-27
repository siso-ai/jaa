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
  // Create a users table
  runner.emit(new Event('create_table_execute', {
    table: 'users',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'text' },
      { name: 'age', type: 'integer' }
    ]
  }));
}

function getRow(table, id) {
  const hash = refs.get(`db/tables/${table}/rows/${id}`);
  return hash ? store.get(hash) : null;
}

// ── insert ──────────────────────────────────────

test('DML: insert creates row with auto-increment id', () => {
  fresh();
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice', age: 30 } }));

  const row = getRow('users', 1);
  assert(row !== null, 'row exists');
  assertEqual(row.id, 1);
  assertEqual(row.name, 'Alice');
  assertEqual(row.age, 30);
});

test('DML: insert increments counter', () => {
  fresh();
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice', age: 30 } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Bob', age: 25 } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Carol', age: 35 } }));

  assertEqual(getRow('users', 1).name, 'Alice');
  assertEqual(getRow('users', 2).name, 'Bob');
  assertEqual(getRow('users', 3).name, 'Carol');

  const counterHash = refs.get('db/tables/users/next_id');
  assertEqual(store.get(counterHash), '3');
});

test('DML: insert into nonexistent table emits error', () => {
  fresh();
  runner.emit(new Event('insert_execute', { table: 'nope', row: { x: 1 } }));
  assert(runner.sample().pending.some(e => e.type === 'error'));
});

test('DML: insert emits row_inserted event', () => {
  fresh();
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice', age: 30 } }));
  const pending = runner.sample().pending;
  const inserted = pending.find(e => e.type === 'row_inserted');
  assert(inserted !== undefined, 'row_inserted event');
  assertEqual(inserted.data.table, 'users');
  assertEqual(inserted.data.id, 1);
});

// ── update ──────────────────────────────────────

test('DML: update by where clause', () => {
  fresh();
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice', age: 30 } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Bob', age: 25 } }));

  runner.emit(new Event('update_execute', {
    table: 'users',
    changes: { age: 31 },
    where: { column: 'name', op: '=', value: 'Alice' }
  }));

  assertEqual(getRow('users', 1).age, 31);
  assertEqual(getRow('users', 2).age, 25); // untouched
});

test('DML: update emits row_updated event', () => {
  fresh();
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice', age: 30 } }));
  runner.emit(new Event('update_execute', {
    table: 'users',
    changes: { age: 31 },
    where: { column: 'name', op: '=', value: 'Alice' }
  }));

  const pending = runner.sample().pending;
  const updated = pending.find(e => e.type === 'row_updated');
  assert(updated !== undefined);
  assertEqual(updated.data.ids[0], 1);
});

test('DML: update preserves old row in store (immutability)', () => {
  fresh();
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice', age: 30 } }));
  const oldHash = refs.get('db/tables/users/rows/1');

  runner.emit(new Event('update_execute', {
    table: 'users',
    changes: { age: 31 },
    where: { column: 'name', op: '=', value: 'Alice' }
  }));

  const newHash = refs.get('db/tables/users/rows/1');
  assert(oldHash !== newHash, 'ref swung to new hash');
  assertEqual(store.get(oldHash).age, 30); // old still in store
  assertEqual(store.get(newHash).age, 31); // new via ref
});

// ── delete ──────────────────────────────────────

test('DML: delete by where clause', () => {
  fresh();
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice', age: 30 } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Bob', age: 25 } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Carol', age: 35 } }));

  runner.emit(new Event('delete_execute', {
    table: 'users',
    where: { column: 'age', op: '<', value: 30 }
  }));

  assertEqual(getRow('users', 2), null); // Bob deleted
  assert(getRow('users', 1) !== null);   // Alice remains
  assert(getRow('users', 3) !== null);   // Carol remains
});

test('DML: delete emits row_deleted with ids', () => {
  fresh();
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice', age: 30 } }));
  runner.emit(new Event('delete_execute', {
    table: 'users',
    where: { column: 'name', op: '=', value: 'Alice' }
  }));

  const deleted = runner.sample().pending.find(e => e.type === 'row_deleted');
  assert(deleted !== undefined);
  assertEqual(deleted.data.ids[0], 1);
});

test('DML: deleted row persists in store (immutability)', () => {
  fresh();
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice', age: 30 } }));
  const hash = refs.get('db/tables/users/rows/1');

  runner.emit(new Event('delete_execute', {
    table: 'users',
    where: { column: 'name', op: '=', value: 'Alice' }
  }));

  assertEqual(refs.get('db/tables/users/rows/1'), null); // ref gone
  assertEqual(store.get(hash).name, 'Alice');             // object stays
});

const exitCode = report('dml-integration');
process.exit(exitCode);
