/**
 * ICE Database — Phase 10 JS Test Suite
 * Transactions, Multi-row INSERT, ALTER TABLE, full parity with PHP Phase 9.
 */
import { test, assert, assertEqual, report } from './runner.js';
import { Runner } from '../src/resolution/Runner.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { MemoryRefs } from '../src/persistence/Refs.js';
import { Event } from '../src/core/Event.js';
import { registerDatabaseGates } from '../src/gates/database/register.js';
import { registerSQLGates } from '../src/gates/query/sql/register.js';

function freshRunner() {
  const r = new Runner({ store: new MemoryStore(), refs: new MemoryRefs() });
  registerDatabaseGates(r);
  registerSQLGates(r);
  return r;
}

function sql(r, query) { r.emit(new Event('sql', { sql: query })); }
function pending(r) { return r.sample().pending; }
function ofType(r, type) { return pending(r).filter(e => e.type === type); }
function queryRows(r, query) {
  r.clearPending();
  sql(r, query);
  const results = ofType(r, 'query_result');
  return results.length > 0 ? results[results.length - 1].data.rows : [];
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// TRANSACTION TESTS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('BEGIN emits transaction_begun', () => {
  const r = freshRunner();
  sql(r, 'BEGIN');
  assert(ofType(r, 'transaction_begun').length > 0);
});

test('BEGIN via direct event', () => {
  const r = freshRunner();
  r.emit(new Event('transaction_begin', {}));
  assert(ofType(r, 'transaction_begun').length > 0);
});

test('double BEGIN errors', () => {
  const r = freshRunner();
  sql(r, 'BEGIN');
  r.clearPending();
  sql(r, 'BEGIN');
  assert(ofType(r, 'error').length > 0);
});

test('COMMIT after BEGIN succeeds', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  sql(r, 'BEGIN');
  sql(r, 'INSERT INTO t (x) VALUES (1)');
  r.clearPending();
  sql(r, 'COMMIT');
  assert(ofType(r, 'transaction_committed').length > 0);
  const rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows.length, 1);
  assertEqual(rows[0].x, 1);
});

test('COMMIT without BEGIN errors', () => {
  const r = freshRunner();
  sql(r, 'COMMIT');
  assert(ofType(r, 'error').length > 0);
});

test('ROLLBACK reverts INSERT', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  sql(r, 'INSERT INTO t (x) VALUES (1)');
  sql(r, 'BEGIN');
  sql(r, 'INSERT INTO t (x) VALUES (2)');
  sql(r, 'INSERT INTO t (x) VALUES (3)');

  let rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows.length, 3);

  sql(r, 'ROLLBACK');
  rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows.length, 1);
  assertEqual(rows[0].x, 1);
});

test('ROLLBACK reverts UPDATE', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  sql(r, 'INSERT INTO t (x) VALUES (10)');

  sql(r, 'BEGIN');
  sql(r, 'UPDATE t SET x = 99 WHERE x = 10');

  let rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows[0].x, 99);

  sql(r, 'ROLLBACK');
  rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows[0].x, 10);
});

test('ROLLBACK reverts DELETE', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  sql(r, 'INSERT INTO t (x) VALUES (1)');
  sql(r, 'INSERT INTO t (x) VALUES (2)');

  sql(r, 'BEGIN');
  sql(r, 'DELETE FROM t WHERE x = 1');
  let rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows.length, 1);

  sql(r, 'ROLLBACK');
  rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows.length, 2);
});

test('ROLLBACK reverts CREATE TABLE', () => {
  const r = freshRunner();
  sql(r, 'BEGIN');
  sql(r, 'CREATE TABLE t (x INTEGER)');
  sql(r, 'INSERT INTO t (x) VALUES (1)');

  sql(r, 'ROLLBACK');
  const rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows.length, 0);
});

test('ROLLBACK reverts DROP TABLE', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  sql(r, 'INSERT INTO t (x) VALUES (42)');

  sql(r, 'BEGIN');
  sql(r, 'DROP TABLE t');
  sql(r, 'ROLLBACK');

  const rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows.length, 1);
  assertEqual(rows[0].x, 42);
});

test('ROLLBACK without BEGIN errors', () => {
  const r = freshRunner();
  sql(r, 'ROLLBACK');
  assert(ofType(r, 'error').length > 0);
});

test('COMMIT then ROLLBACK errors', () => {
  const r = freshRunner();
  sql(r, 'BEGIN');
  sql(r, 'COMMIT');
  r.clearPending();
  sql(r, 'ROLLBACK');
  assert(ofType(r, 'error').length > 0);
});

test('multiple transactions sequentially', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');

  sql(r, 'BEGIN');
  sql(r, 'INSERT INTO t (x) VALUES (1)');
  sql(r, 'COMMIT');

  sql(r, 'BEGIN');
  sql(r, 'INSERT INTO t (x) VALUES (2)');
  sql(r, 'ROLLBACK');

  sql(r, 'BEGIN');
  sql(r, 'INSERT INTO t (x) VALUES (3)');
  sql(r, 'COMMIT');

  const rows = queryRows(r, 'SELECT * FROM t ORDER BY x');
  assertEqual(rows.length, 2);
  assertEqual(rows[0].x, 1);
  assertEqual(rows[1].x, 3);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MULTI-ROW INSERT TESTS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('INSERT multiple rows via VALUES', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (name TEXT, age INTEGER)');
  sql(r, "INSERT INTO t (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)");
  const rows = queryRows(r, 'SELECT * FROM t ORDER BY age');
  assertEqual(rows.length, 3);
  assertEqual(rows[0].name, 'Bob');
  assertEqual(rows[1].name, 'Alice');
  assertEqual(rows[2].name, 'Charlie');
});

test('multi-row INSERT assigns sequential IDs', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  sql(r, 'INSERT INTO t (x) VALUES (10), (20), (30)');
  const rows = queryRows(r, 'SELECT * FROM t ORDER BY id');
  assertEqual(rows.length, 3);
  assertEqual(rows[0].id, 1);
  assertEqual(rows[1].id, 2);
  assertEqual(rows[2].id, 3);
});

test('multi-row INSERT with defaults', () => {
  const r = freshRunner();
  sql(r, "CREATE TABLE t (status TEXT DEFAULT 'active', value INTEGER)");
  sql(r, 'INSERT INTO t (value) VALUES (1), (2)');
  const rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows.length, 2);
  assertEqual(rows[0].status, 'active');
  assertEqual(rows[1].status, 'active');
});

test('single-row INSERT still works', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  sql(r, 'INSERT INTO t (x) VALUES (42)');
  const rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows.length, 1);
  assertEqual(rows[0].x, 42);
});

test('multi-row INSERT 10 rows', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  const values = Array.from({ length: 10 }, (_, i) => `(${i + 1})`).join(', ');
  sql(r, `INSERT INTO t (x) VALUES ${values}`);
  const rows = queryRows(r, 'SELECT COUNT(*) AS cnt FROM t');
  assertEqual(rows[0].cnt, 10);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ALTER TABLE / ADD COLUMN TESTS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('ADD COLUMN via SQL', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (name TEXT)');
  sql(r, "INSERT INTO t (name) VALUES ('Alice')");
  r.clearPending();
  sql(r, 'ALTER TABLE t ADD COLUMN age INTEGER');
  assert(ofType(r, 'column_added').length > 0);
});

test('ADD COLUMN backfills NULL', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (name TEXT)');
  sql(r, "INSERT INTO t (name) VALUES ('Alice')");
  sql(r, "INSERT INTO t (name) VALUES ('Bob')");
  sql(r, 'ALTER TABLE t ADD COLUMN age INTEGER');
  const rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows.length, 2);
  assertEqual(rows[0].age, null);
  assertEqual(rows[1].age, null);
});

test('ADD COLUMN with DEFAULT backfills value', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (name TEXT)');
  sql(r, "INSERT INTO t (name) VALUES ('Alice')");
  sql(r, "ALTER TABLE t ADD COLUMN status TEXT DEFAULT 'active'");
  const rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows[0].status, 'active');
});

test('ADD COLUMN then INSERT uses new column', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (name TEXT)');
  sql(r, 'ALTER TABLE t ADD COLUMN score INTEGER');
  sql(r, "INSERT INTO t (name, score) VALUES ('Alice', 100)");
  const rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows[0].score, 100);
});

test('ADD COLUMN duplicate error', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (name TEXT)');
  r.clearPending();
  sql(r, 'ALTER TABLE t ADD COLUMN name TEXT');
  assert(ofType(r, 'error').length > 0);
});

test('ADD COLUMN to nonexistent table error', () => {
  const r = freshRunner();
  sql(r, 'ALTER TABLE nope ADD COLUMN x INTEGER');
  assert(ofType(r, 'error').length > 0);
});

test('ADD COLUMN keyword is optional', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (a INTEGER)');
  r.clearPending();
  sql(r, 'ALTER TABLE t ADD b TEXT');
  assert(ofType(r, 'column_added').length > 0);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ALTER TABLE / DROP COLUMN TESTS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('DROP COLUMN via SQL', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (name TEXT, age INTEGER)');
  sql(r, "INSERT INTO t (name, age) VALUES ('Alice', 30)");
  r.clearPending();
  sql(r, 'ALTER TABLE t DROP COLUMN age');
  assert(ofType(r, 'column_dropped').length > 0);
});

test('DROP COLUMN removes from rows', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (name TEXT, age INTEGER, score REAL)');
  sql(r, "INSERT INTO t (name, age, score) VALUES ('Alice', 30, 95.5)");
  sql(r, 'ALTER TABLE t DROP COLUMN age');
  const rows = queryRows(r, 'SELECT * FROM t');
  assert(rows[0].age === undefined || rows[0].age === null);
  assertEqual(rows[0].name, 'Alice');
  assert(Math.abs(rows[0].score - 95.5) < 0.01);
});

test('DROP COLUMN nonexistent error', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (name TEXT)');
  r.clearPending();
  sql(r, 'ALTER TABLE t DROP COLUMN nope');
  assert(ofType(r, 'error').length > 0);
});

test('DROP COLUMN id not allowed', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (name TEXT)');
  sql(r, "INSERT INTO t (name) VALUES ('x')");
  r.clearPending();
  r.emit(new Event('alter_table_drop_column', { table: 't', column: 'id' }));
  assert(ofType(r, 'error').length > 0);
});

test('DROP COLUMN on nonexistent table error', () => {
  const r = freshRunner();
  sql(r, 'ALTER TABLE nope DROP COLUMN x');
  assert(ofType(r, 'error').length > 0);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ALTER TABLE / RENAME TESTS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('RENAME TABLE via SQL', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE old_name (x INTEGER)');
  sql(r, 'INSERT INTO old_name (x) VALUES (42)');
  r.clearPending();
  sql(r, 'ALTER TABLE old_name RENAME TO new_name');
  assert(ofType(r, 'table_renamed').length > 0);
});

test('RENAME TABLE data persists', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE alpha (x INTEGER)');
  sql(r, 'INSERT INTO alpha (x) VALUES (1)');
  sql(r, 'INSERT INTO alpha (x) VALUES (2)');
  sql(r, 'ALTER TABLE alpha RENAME TO beta');

  let rows = queryRows(r, 'SELECT * FROM beta');
  assertEqual(rows.length, 2);

  rows = queryRows(r, 'SELECT * FROM alpha');
  assertEqual(rows.length, 0);
});

test('RENAME TABLE nonexistent error', () => {
  const r = freshRunner();
  r.emit(new Event('rename_table', { table: 'nope', newName: 'x' }));
  assert(ofType(r, 'error').length > 0);
});

test('RENAME TABLE to existing name error', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE a (x INTEGER)');
  sql(r, 'CREATE TABLE b (y INTEGER)');
  r.clearPending();
  r.emit(new Event('rename_table', { table: 'a', newName: 'b' }));
  assert(ofType(r, 'error').length > 0);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// TRANSACTION + ALTER TABLE INTEGRATION
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('ROLLBACK reverts ADD COLUMN', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (name TEXT)');
  sql(r, "INSERT INTO t (name) VALUES ('Alice')");

  sql(r, 'BEGIN');
  sql(r, 'ALTER TABLE t ADD COLUMN age INTEGER');
  sql(r, 'ROLLBACK');

  const rows = queryRows(r, 'SELECT * FROM t');
  assertEqual(rows.length, 1);
  assert(rows[0].age === undefined || rows[0].age === null);
});

test('ROLLBACK reverts RENAME TABLE', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE original (x INTEGER)');
  sql(r, 'INSERT INTO original (x) VALUES (1)');

  sql(r, 'BEGIN');
  sql(r, 'ALTER TABLE original RENAME TO renamed');
  let rows = queryRows(r, 'SELECT * FROM renamed');
  assertEqual(rows.length, 1);

  sql(r, 'ROLLBACK');
  rows = queryRows(r, 'SELECT * FROM original');
  assertEqual(rows.length, 1);
  rows = queryRows(r, 'SELECT * FROM renamed');
  assertEqual(rows.length, 0);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// END-TO-END INTEGRATION
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('full workflow: schema evolution + transactions', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE products (name TEXT, price INTEGER)');
  sql(r, "INSERT INTO products (name, price) VALUES ('Widget', 10), ('Gadget', 20)");

  sql(r, "ALTER TABLE products ADD COLUMN category TEXT DEFAULT 'general'");
  sql(r, 'ALTER TABLE products ADD COLUMN in_stock BOOLEAN DEFAULT TRUE');

  let rows = queryRows(r, 'SELECT * FROM products');
  assertEqual(rows.length, 2);
  assertEqual(rows[0].category, 'general');

  sql(r, 'BEGIN');
  sql(r, 'DELETE FROM products');
  rows = queryRows(r, 'SELECT * FROM products');
  assertEqual(rows.length, 0);

  sql(r, 'ROLLBACK');
  rows = queryRows(r, 'SELECT * FROM products');
  assertEqual(rows.length, 2);

  sql(r, 'BEGIN');
  sql(r, "UPDATE products SET price = 15 WHERE name = 'Widget'");
  sql(r, "INSERT INTO products (name, price, category) VALUES ('Doohickey', 30, 'premium')");
  sql(r, 'COMMIT');

  rows = queryRows(r, 'SELECT * FROM products ORDER BY price');
  assertEqual(rows.length, 3);
  assertEqual(rows[0].price, 15);
  assertEqual(rows[2].name, 'Doohickey');
});

test('rename + insert + query', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE tmp (x INTEGER)');
  sql(r, 'INSERT INTO tmp (x) VALUES (1), (2), (3)');
  sql(r, 'ALTER TABLE tmp RENAME TO final_table');
  sql(r, 'INSERT INTO final_table (x) VALUES (4)');
  const rows = queryRows(r, 'SELECT * FROM final_table ORDER BY x');
  assertEqual(rows.length, 4);
  assertEqual(rows[3].x, 4);
});

test('drop column + aggregate query', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE scores (player TEXT, score INTEGER, timestamp TEXT)');
  sql(r, "INSERT INTO scores (player, score, timestamp) VALUES ('A', 100, '2024-01-01'), ('B', 200, '2024-01-02'), ('A', 150, '2024-01-03')");
  sql(r, 'ALTER TABLE scores DROP COLUMN timestamp');
  const rows = queryRows(r, 'SELECT SUM(score) AS total FROM scores GROUP BY player');
  assertEqual(rows.length, 2);
  const totals = rows.map(r => r.total).sort((a, b) => a - b);
  assertEqual(totals[0], 200);
  assertEqual(totals[1], 250);
});

report('phase10-parity');
