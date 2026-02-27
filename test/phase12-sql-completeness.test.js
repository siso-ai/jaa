/**
 * ICE Database — Phase 12 JS Test Suite
 * SQL Completeness: UPDATE expressions, INSERT...SELECT, CTAS, IF NOT EXISTS, EXPLAIN.
 */
import { test, assert, assertEqual, report, section } from './runner.js';
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
function queryRows(r, query) {
  r.clearPending();
  sql(r, query);
  const results = r.sample().pending.filter(e => e.type === 'query_result');
  return results.length > 0 ? results[results.length - 1].data.rows : [];
}
function lastEvent(r, type) {
  const events = r.sample().pending.filter(e => e.type === type);
  return events.length > 0 ? events[events.length - 1] : null;
}
function hasError(r) { return lastEvent(r, 'error') !== null; }

function setupProducts(r) {
  sql(r, 'CREATE TABLE products (name TEXT, price INTEGER, qty INTEGER)');
  sql(r, "INSERT INTO products (name, price, qty) VALUES ('Widget', 10, 5)");
  sql(r, "INSERT INTO products (name, price, qty) VALUES ('Gadget', 25, 3)");
  sql(r, "INSERT INTO products (name, price, qty) VALUES ('Doohickey', 7, 12)");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// UPDATE WITH EXPRESSIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('UPDATE with expressions');

test('UPDATE SET col = col + literal', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, "UPDATE products SET price = price + 5 WHERE name = 'Widget'");
  const rows = queryRows(r, "SELECT price FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].price, 15);
});

test('UPDATE SET col = col * literal', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, 'UPDATE products SET price = price * 2');
  const rows = queryRows(r, 'SELECT name, price FROM products ORDER BY name');
  assertEqual(rows[0].price, 14);  // Doohickey 7*2
  assertEqual(rows[1].price, 50);  // Gadget 25*2
  assertEqual(rows[2].price, 20);  // Widget 10*2
});

test('UPDATE SET col = expression referencing other cols', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, "UPDATE products SET qty = price + qty WHERE name = 'Widget'");
  const rows = queryRows(r, "SELECT qty FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].qty, 15);
});

test('UPDATE SET with function', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, "UPDATE products SET name = UPPER(name) WHERE name = 'Widget'");
  const rows = queryRows(r, "SELECT name FROM products WHERE name = 'WIDGET'");
  assertEqual(rows.length, 1);
});

test('UPDATE mixed literal and expression SET', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, "UPDATE products SET price = price + 1, name = 'Updated' WHERE name = 'Widget'");
  const rows = queryRows(r, "SELECT name, price FROM products WHERE name = 'Updated'");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].price, 11);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// IF NOT EXISTS / IF EXISTS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('IF NOT EXISTS / IF EXISTS');

test('CREATE TABLE IF NOT EXISTS on new table', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE IF NOT EXISTS t (x INTEGER)');
  assert(lastEvent(r, 'table_created') !== null);
});

test('CREATE TABLE IF NOT EXISTS on existing table', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  r.clearPending();
  sql(r, 'CREATE TABLE IF NOT EXISTS t (x INTEGER)');
  assert(!hasError(r));
  assert(lastEvent(r, 'table_exists') !== null);
});

test('CREATE TABLE without IF NOT EXISTS errors on duplicate', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  r.clearPending();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  assert(hasError(r));
});

test('DROP TABLE IF EXISTS on missing table', () => {
  const r = freshRunner();
  r.clearPending();
  sql(r, 'DROP TABLE IF EXISTS nonexistent');
  assert(!hasError(r));
});

test('DROP TABLE IF EXISTS on existing table', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  r.clearPending();
  sql(r, 'DROP TABLE IF EXISTS t');
  assert(!hasError(r));
  assert(lastEvent(r, 'table_dropped') !== null);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// INSERT...SELECT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('INSERT...SELECT');

test('INSERT INTO t SELECT * FROM source', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, 'CREATE TABLE archive (name TEXT, price INTEGER, qty INTEGER)');
  sql(r, 'INSERT INTO archive SELECT name, price, qty FROM products');
  const rows = queryRows(r, 'SELECT * FROM archive');
  assertEqual(rows.length, 3);
});

test('INSERT...SELECT with WHERE', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, 'CREATE TABLE expensive (name TEXT, price INTEGER)');
  sql(r, 'INSERT INTO expensive (name, price) SELECT name, price FROM products WHERE price > 8');
  const rows = queryRows(r, 'SELECT * FROM expensive ORDER BY price');
  assertEqual(rows.length, 2);
  assertEqual(rows[0].name, 'Widget');
  assertEqual(rows[1].name, 'Gadget');
});

test('INSERT...SELECT with expression', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, 'CREATE TABLE revenue (name TEXT, rev INTEGER)');
  sql(r, 'INSERT INTO revenue (name, rev) SELECT name, price * qty FROM products');
  const rows = queryRows(r, 'SELECT * FROM revenue ORDER BY rev DESC');
  assertEqual(rows.length, 3);
  assertEqual(rows[0].rev, 84);
});

test('INSERT...SELECT assigns new IDs', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, 'CREATE TABLE copy (name TEXT)');
  sql(r, 'INSERT INTO copy (name) SELECT name FROM products');
  const rows = queryRows(r, 'SELECT id, name FROM copy ORDER BY id');
  assertEqual(rows[0].id, 1);
  assertEqual(rows[1].id, 2);
  assertEqual(rows[2].id, 3);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CREATE TABLE AS SELECT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('CREATE TABLE AS SELECT');

test('CTAS basic', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, 'CREATE TABLE backup AS SELECT name, price FROM products');
  const rows = queryRows(r, 'SELECT * FROM backup ORDER BY name');
  assertEqual(rows.length, 3);
  assertEqual(rows[0].name, 'Doohickey');
  assertEqual(rows[0].price, 7);
});

test('CTAS with WHERE', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, 'CREATE TABLE cheap AS SELECT name, price FROM products WHERE price < 10');
  const rows = queryRows(r, 'SELECT * FROM cheap');
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Doohickey');
});

test('CTAS with expression', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, 'CREATE TABLE summary AS SELECT name, price * qty AS revenue FROM products');
  const rows = queryRows(r, 'SELECT * FROM summary ORDER BY revenue DESC');
  assertEqual(rows.length, 3);
  assertEqual(rows[0].revenue, 84);
});

test('CTAS IF NOT EXISTS on existing table', () => {
  const r = freshRunner();
  setupProducts(r);
  r.clearPending();
  sql(r, 'CREATE TABLE IF NOT EXISTS products AS SELECT name FROM products');
  assert(!hasError(r));
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// EXPLAIN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('EXPLAIN');

test('EXPLAIN simple SELECT', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, 'EXPLAIN SELECT * FROM products');
  assert(rows.length >= 1);
  assertEqual(rows[0].operation, 'SCAN products');
});

test('EXPLAIN SELECT with WHERE', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, 'EXPLAIN SELECT name FROM products WHERE price > 10');
  const ops = rows.map(r => r.operation);
  assert(ops.includes('SCAN products'));
  assert(ops.includes('FILTER'));
});

test('EXPLAIN SELECT with JOIN', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, 'CREATE TABLE orders (product TEXT, amount INTEGER)');
  const rows = queryRows(r, 'EXPLAIN SELECT * FROM products JOIN orders ON name = product');
  const ops = rows.map(r => r.operation);
  assert(ops.includes('SCAN products'));
  assert(ops.some(op => op.includes('JOIN')));
});

test('EXPLAIN SELECT with ORDER BY and LIMIT', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, 'EXPLAIN SELECT name FROM products ORDER BY price LIMIT 2');
  const ops = rows.map(r => r.operation);
  assert(ops.includes('ORDER BY'));
  assert(ops.some(op => op.startsWith('LIMIT')));
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// END-TO-END INTEGRATION
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('End-to-End Integration');

test('UPDATE + INSERT...SELECT + query', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, 'UPDATE products SET price = price * 2 WHERE qty <= 5');
  sql(r, 'CREATE TABLE report AS SELECT name, price FROM products WHERE price > 15');
  const rows = queryRows(r, 'SELECT * FROM report ORDER BY price DESC');
  assertEqual(rows.length, 2);
  assertEqual(rows[0].name, 'Gadget');  // 50
  assertEqual(rows[1].name, 'Widget');  // 20
});

test('CTAS + INSERT...SELECT chained', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE source (x INTEGER)');
  sql(r, 'INSERT INTO source (x) VALUES (1), (2), (3)');
  sql(r, 'CREATE TABLE doubled AS SELECT x * 2 AS val FROM source');
  sql(r, 'CREATE TABLE tripled (val INTEGER)');
  sql(r, 'INSERT INTO tripled (val) SELECT val FROM doubled');
  const rows = queryRows(r, 'SELECT val FROM tripled ORDER BY val');
  assertEqual(rows[0].val, 2);
  assertEqual(rows[1].val, 4);
  assertEqual(rows[2].val, 6);
});

report('phase12-sql-completeness');
