import { test, assert, assertEqual, report } from './runner.js';
import { Runner } from '../src/resolution/Runner.js';
import { FileStore } from '../src/persistence/FileStore.js';
import { FileRefs } from '../src/persistence/FileRefs.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { Recovery } from '../src/persistence/Recovery.js';
import { registerDatabaseGates } from '../src/gates/database/register.js';
import { registerSQLGates } from '../src/gates/query/sql/register.js';
import { canonicalize } from '../src/persistence/canonicalize.js';
import { createHash } from 'crypto';
import { Event } from '../src/core/Event.js';
import { existsSync, mkdirSync, rmSync, writeFileSync } from 'fs';
import { join } from 'path';

const BASE = '/tmp/siso-test-file-integration-' + process.pid;

function fresh() {
  if (existsSync(BASE)) rmSync(BASE, { recursive: true });
  mkdirSync(BASE, { recursive: true });
}

function makeRunner() {
  const store = new FileStore({ basePath: BASE });
  const refs = new FileRefs({ basePath: BASE });
  const runner = new Runner({ store, refs });
  registerDatabaseGates(runner);
  registerSQLGates(runner);
  return { store, refs, runner };
}

// ── data persists across Runner instances ────────

test('FilePersistence: data persists across Runner instances', () => {
  fresh();

  // Runner 1: create and populate
  const { runner: r1 } = makeRunner();
  r1.emit(new Event('sql', { sql: 'CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)' }));
  r1.emit(new Event('sql', { sql: "INSERT INTO users (name, age) VALUES ('Alice', 30)" }));
  r1.emit(new Event('sql', { sql: "INSERT INTO users (name, age) VALUES ('Bob', 25)" }));

  // Runner 2: new runner, same basePath — reads persisted data
  const { runner: r2 } = makeRunner();
  r2.emit(new Event('sql', { sql: 'SELECT * FROM users' }));

  const result = r2.sample().pending.find(e => e.type === 'query_result');
  assert(result !== undefined, 'query_result from second runner');
  assertEqual(result.data.rows.length, 2);
  assert(result.data.rows.some(r => r.name === 'Alice'));
  assert(result.data.rows.some(r => r.name === 'Bob'));
});

// ── full CRUD lifecycle with file persistence ───

test('FilePersistence: full CRUD lifecycle', () => {
  fresh();
  const { runner, store, refs } = makeRunner();

  // Create
  runner.emit(new Event('sql', { sql: 'CREATE TABLE products (id INTEGER, name TEXT, price REAL)' }));
  assert(refs.get('db/tables/products/schema') !== null);

  // Insert
  runner.emit(new Event('sql', { sql: "INSERT INTO products (name, price) VALUES ('Widget', 9.99)" }));
  runner.emit(new Event('sql', { sql: "INSERT INTO products (name, price) VALUES ('Gadget', 24.99)" }));

  // Read
  runner.emit(new Event('sql', { sql: 'SELECT * FROM products WHERE price > 10' }));
  let result = runner.sample().pending.find(e => e.type === 'query_result');
  assertEqual(result.data.rows.length, 1);
  assertEqual(result.data.rows[0].name, 'Gadget');

  // Update
  runner.emit(new Event('sql', { sql: "UPDATE products SET price = 19.99 WHERE name = 'Widget'" }));

  // Verify with new runner
  const { runner: r2 } = makeRunner();
  r2.emit(new Event('sql', { sql: 'SELECT * FROM products ORDER BY price ASC' }));
  result = r2.sample().pending.find(e => e.type === 'query_result');
  assertEqual(result.data.rows[0].price, 19.99);
  assertEqual(result.data.rows[1].price, 24.99);

  // Delete
  r2.emit(new Event('sql', { sql: "DELETE FROM products WHERE name = 'Widget'" }));

  // Verify with yet another runner
  const { runner: r3 } = makeRunner();
  r3.emit(new Event('sql', { sql: 'SELECT * FROM products' }));
  result = r3.sample().pending.find(e => e.type === 'query_result');
  assertEqual(result.data.rows.length, 1);
  assertEqual(result.data.rows[0].name, 'Gadget');
});

// ── crash recovery simulation ───────────────────

test('FilePersistence: crash recovery — incomplete batch is replayed', () => {
  fresh();
  const { store, refs } = makeRunner();

  // Simulate: a table was created, but a row insert crashed mid-batch
  // The store put completed, but the ref set didn't
  const row = { id: 1, name: 'Alice', age: 30 };
  const rowHash = store.put(row);
  const counter = store.put('1');

  // Schema was fully committed
  const schema = { name: 'users', columns: [{ name: 'id', type: 'integer' }, { name: 'name', type: 'text' }, { name: 'age', type: 'integer' }] };
  const schemaHash = store.put(schema);
  refs.set('db/tables/users/schema', schemaHash);

  const counterZero = store.put('0');
  refs.set('db/tables/users/next_id', counterZero);

  // Write a WAL entry simulating the crash
  const recovery = new Recovery(BASE);
  mkdirSync(join(BASE, 'wal'), { recursive: true });
  writeFileSync(join(BASE, 'wal', 'pending.json'), JSON.stringify({
    timestamp: Date.now(),
    puts: [
      { hash: rowHash, content: canonicalize(row), applied: true },
      { hash: counter, content: canonicalize('1'), applied: true },
    ],
    refSets: [
      { name: 'db/tables/users/rows/1', hash: rowHash, applied: false },
      { name: 'db/tables/users/next_id', hash: counter, applied: false },
    ],
    refDeletes: [],
  }));

  // Row ref should NOT exist yet (crash happened before ref set)
  assertEqual(refs.get('db/tables/users/rows/1'), null);

  // Recover
  recovery.recover(store, refs);

  // Now row ref should exist
  assertEqual(refs.get('db/tables/users/rows/1'), rowHash);
  assertEqual(refs.get('db/tables/users/next_id'), counter);

  // New runner can read the recovered data
  const { runner } = makeRunner();
  runner.emit(new Event('sql', { sql: 'SELECT * FROM users' }));
  const result = runner.sample().pending.find(e => e.type === 'query_result');
  assert(result !== undefined);
  assertEqual(result.data.rows.length, 1);
  assertEqual(result.data.rows[0].name, 'Alice');
});

// ── file store objects inspectable with fs ──────

test('FilePersistence: directory structure is inspectable', () => {
  fresh();
  const { runner, refs } = makeRunner();
  runner.emit(new Event('sql', { sql: 'CREATE TABLE logs (id INTEGER, message TEXT)' }));
  runner.emit(new Event('sql', { sql: "INSERT INTO logs (message) VALUES ('hello world')" }));

  // Schema ref file exists at expected path
  assert(existsSync(join(BASE, 'refs', 'db', 'tables', 'logs', 'schema')));
  assert(existsSync(join(BASE, 'refs', 'db', 'tables', 'logs', 'next_id')));
  assert(existsSync(join(BASE, 'refs', 'db', 'tables', 'logs', 'rows', '1')));
});

// ── mixed MemoryStore + FileRefs ────────────────

test('FilePersistence: mixed Memory+File backends work', () => {
  fresh();
  const memStore = new MemoryStore();
  const fileRefs = new FileRefs({ basePath: BASE });
  const runner = new Runner({ store: memStore, refs: fileRefs });
  registerDatabaseGates(runner);

  runner.emit(new Event('create_table_execute', {
    table: 'mixed',
    columns: [{ name: 'id', type: 'integer' }, { name: 'val', type: 'text' }]
  }));

  // Ref is on disk
  assert(existsSync(join(BASE, 'refs', 'db', 'tables', 'mixed', 'schema')));
  // Object is in memory
  const hash = fileRefs.get('db/tables/mixed/schema');
  assert(memStore.has(hash));
});

// cleanup
if (existsSync(BASE)) rmSync(BASE, { recursive: true });

const exitCode = report('file-persistence-integration');
process.exit(exitCode);
