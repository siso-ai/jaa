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
}

// ── create table ────────────────────────────────

test('DDL: create table stores schema and counter', () => {
  fresh();
  runner.emit(new Event('create_table_execute', {
    table: 'users',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'text' },
      { name: 'age', type: 'integer' }
    ]
  }));

  const schemaHash = refs.get('db/tables/users/schema');
  assert(schemaHash !== null, 'schema ref exists');
  const schema = store.get(schemaHash);
  assertEqual(schema.name, 'users');
  assertEqual(schema.columns.length, 3);

  const counterHash = refs.get('db/tables/users/next_id');
  assertEqual(store.get(counterHash), '0');

  const pending = runner.sample().pending;
  assert(pending.some(e => e.type === 'table_created'));
});

test('DDL: create duplicate table emits error', () => {
  fresh();
  runner.emit(new Event('create_table_execute', { table: 'users', columns: [] }));
  runner.emit(new Event('create_table_execute', { table: 'users', columns: [] }));
  const pending = runner.sample().pending;
  assert(pending.some(e => e.type === 'error' && e.data.message.includes('already exists')));
});

// ── drop table ──────────────────────────────────

test('DDL: drop table removes schema, counter, and rows', () => {
  fresh();
  // Create and populate
  runner.emit(new Event('create_table_execute', {
    table: 'users',
    columns: [{ name: 'id', type: 'integer' }, { name: 'name', type: 'text' }]
  }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice' } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Bob' } }));

  assert(refs.get('db/tables/users/rows/1') !== null, 'row exists before drop');

  runner.emit(new Event('drop_table_execute', { table: 'users' }));

  assertEqual(refs.get('db/tables/users/schema'), null);
  assertEqual(refs.get('db/tables/users/next_id'), null);
  assertEqual(refs.get('db/tables/users/rows/1'), null);
  assertEqual(refs.get('db/tables/users/rows/2'), null);

  const pending = runner.sample().pending;
  assert(pending.some(e => e.type === 'table_dropped'));
});

test('DDL: drop nonexistent table emits error', () => {
  fresh();
  runner.emit(new Event('drop_table_execute', { table: 'nope' }));
  assert(runner.sample().pending.some(e => e.type === 'error'));
});

test('DDL: drop nonexistent table with ifExists is silent', () => {
  fresh();
  runner.emit(new Event('drop_table_execute', { table: 'nope', ifExists: true }));
  assert(!runner.sample().pending.some(e => e.type === 'error'));
});

const exitCode = report('ddl-integration');
process.exit(exitCode);
