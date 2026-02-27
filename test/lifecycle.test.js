import { test, assert, assertEqual, report } from './runner.js';
import { Runner } from '../src/resolution/Runner.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { MemoryRefs } from '../src/persistence/Refs.js';
import { registerDatabaseGates } from '../src/gates/database/register.js';
import { Event } from '../src/core/Event.js';

// ── full CRUD lifecycle ─────────────────────────

test('Lifecycle: create table → insert → update → scan → delete → scan', () => {
  const store = new MemoryStore();
  const refs = new MemoryRefs();
  const runner = new Runner({ store, refs });
  registerDatabaseGates(runner);

  // 1. Create table
  runner.emit(new Event('create_table_execute', {
    table: 'products',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'text' },
      { name: 'price', type: 'real' },
      { name: 'in_stock', type: 'boolean' }
    ]
  }));
  assert(refs.get('db/tables/products/schema') !== null, 'table created');

  // 2. Insert rows
  runner.emit(new Event('insert_execute', { table: 'products', row: { name: 'Widget', price: 9.99, in_stock: true } }));
  runner.emit(new Event('insert_execute', { table: 'products', row: { name: 'Gadget', price: 24.99, in_stock: true } }));
  runner.emit(new Event('insert_execute', { table: 'products', row: { name: 'Doohickey', price: 4.99, in_stock: false } }));

  // 3. Verify all three exist
  runner.emit(new Event('table_scan', { table: 'products' }));
  let scan = runner.sample().pending.find(e => e.type === 'scan_result');
  assertEqual(scan.data.rows.length, 3);

  // 4. Update: mark Doohickey as in stock
  runner.emit(new Event('update_execute', {
    table: 'products',
    changes: { in_stock: true },
    where: { column: 'name', op: '=', value: 'Doohickey' }
  }));

  // 5. Verify update
  const doohickeyHash = refs.get('db/tables/products/rows/3');
  assertEqual(store.get(doohickeyHash).in_stock, true);

  // 6. Delete: remove Widget
  runner.emit(new Event('delete_execute', {
    table: 'products',
    where: { column: 'name', op: '=', value: 'Widget' }
  }));

  // 7. Scan again
  runner.emit(new Event('table_scan', { table: 'products' }));
  const scans = runner.sample().pending.filter(e => e.type === 'scan_result');
  scan = scans[scans.length - 1]; // latest scan
  assertEqual(scan.data.rows.length, 2);
  assert(scan.data.rows.every(r => r.name !== 'Widget'), 'Widget is gone');
});

// ── multiple tables ─────────────────────────────

test('Lifecycle: multiple tables coexist', () => {
  const store = new MemoryStore();
  const refs = new MemoryRefs();
  const runner = new Runner({ store, refs });
  registerDatabaseGates(runner);

  runner.emit(new Event('create_table_execute', {
    table: 'users',
    columns: [{ name: 'id', type: 'integer' }, { name: 'name', type: 'text' }]
  }));
  runner.emit(new Event('create_table_execute', {
    table: 'orders',
    columns: [{ name: 'id', type: 'integer' }, { name: 'user_id', type: 'integer' }, { name: 'total', type: 'real' }]
  }));

  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice' } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Bob' } }));
  runner.emit(new Event('insert_execute', { table: 'orders', row: { user_id: 1, total: 50.00 } }));
  runner.emit(new Event('insert_execute', { table: 'orders', row: { user_id: 1, total: 30.00 } }));
  runner.emit(new Event('insert_execute', { table: 'orders', row: { user_id: 2, total: 100.00 } }));

  // Scan each table independently
  runner.emit(new Event('table_scan', { table: 'users' }));
  let scan = runner.sample().pending.find(e => e.type === 'scan_result' && e.data.table === 'users');
  assertEqual(scan.data.rows.length, 2);

  runner.emit(new Event('table_scan', { table: 'orders' }));
  scan = runner.sample().pending.find(e => e.type === 'scan_result' && e.data.table === 'orders');
  assertEqual(scan.data.rows.length, 3);

  // Join
  runner.emit(new Event('join', {
    left: { table: 'users' },
    right: { table: 'orders' },
    on: { left: 'id', right: 'user_id' },
    type: 'inner'
  }));
  const joined = runner.sample().pending.find(e => e.type === 'join_result');
  assertEqual(joined.data.rows.length, 3);
  assert(joined.data.rows.every(r => r.name !== undefined && r.total !== undefined));
});

// ── snapshot rollback ───────────────────────────

test('Lifecycle: snapshot + mutations + rollback restores state', () => {
  const store = new MemoryStore();
  const refs = new MemoryRefs();
  const runner = new Runner({ store, refs });
  registerDatabaseGates(runner);

  runner.emit(new Event('create_table_execute', {
    table: 'users',
    columns: [{ name: 'id', type: 'integer' }, { name: 'name', type: 'text' }]
  }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice' } }));

  const snap = runner.snapshot();

  // Mutate after snapshot
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Bob' } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Carol' } }));
  assertEqual(refs.list('db/tables/users/rows/').length, 3);

  // Rollback
  runner.restore(snap);
  assertEqual(refs.list('db/tables/users/rows/').length, 1);

  const hash = refs.get('db/tables/users/rows/1');
  assertEqual(store.get(hash).name, 'Alice');
});

const exitCode = report('lifecycle');
process.exit(exitCode);
