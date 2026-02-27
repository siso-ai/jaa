/**
 * ICE Database — Phase 11 JS Test Suite
 * SQL Expression Engine: arithmetic, functions, CASE WHEN, UNION.
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
function queryRows(r, query) {
  r.clearPending();
  sql(r, query);
  const results = r.sample().pending.filter(e => e.type === 'query_result');
  return results.length > 0 ? results[results.length - 1].data.rows : [];
}

function setupProducts(r) {
  sql(r, 'CREATE TABLE products (name TEXT, price INTEGER, qty INTEGER)');
  sql(r, "INSERT INTO products (name, price, qty) VALUES ('Widget', 10, 5)");
  sql(r, "INSERT INTO products (name, price, qty) VALUES ('Gadget', 25, 3)");
  sql(r, "INSERT INTO products (name, price, qty) VALUES ('Doohickey', 7, 12)");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ARITHMETIC EXPRESSIONS IN SELECT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('SELECT col * col AS alias', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, 'SELECT name, price * qty AS total FROM products ORDER BY total DESC');
  assertEqual(rows.length, 3);
  assertEqual(rows[0].total, 84); // 7*12
  assertEqual(rows[1].total, 75); // 25*3
  assertEqual(rows[2].total, 50); // 10*5
});

test('SELECT col + col', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT name, price + qty AS total_sum FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].total_sum, 15);
});

test('SELECT col - literal', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT name, price - 5 AS discounted FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].discounted, 5);
});

test('SELECT col / literal', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT name, price / 2 AS half_price FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].half_price, 5);
});

test('SELECT parenthesized expression', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT name, (price + 5) * qty AS boosted FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].boosted, 75); // (10+5)*5
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// EXPRESSIONS IN WHERE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('WHERE expr > literal', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, 'SELECT name FROM products WHERE price * qty > 60');
  assertEqual(rows.length, 2);
});

test('WHERE expr = expr', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (a INTEGER, b INTEGER)');
  sql(r, 'INSERT INTO t (a, b) VALUES (5, 5)');
  sql(r, 'INSERT INTO t (a, b) VALUES (3, 7)');
  const rows = queryRows(r, 'SELECT * FROM t WHERE a + b = 10');
  assertEqual(rows.length, 2);
});

test('WHERE col compared to col', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, 'SELECT name FROM products WHERE price > qty');
  assertEqual(rows.length, 2); // Widget 10>5, Gadget 25>3
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SQL BUILT-IN FUNCTIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('UPPER()', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT UPPER(name) AS upper_name FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].upper_name, 'WIDGET');
});

test('LOWER()', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT LOWER(name) AS lower_name FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].lower_name, 'widget');
});

test('LENGTH()', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT LENGTH(name) AS len FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].len, 6);
});

test('ABS()', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x INTEGER)');
  sql(r, 'INSERT INTO t (x) VALUES (-42)');
  const rows = queryRows(r, 'SELECT ABS(x) AS abs_x FROM t');
  assertEqual(rows[0].abs_x, 42);
});

test('ROUND()', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x REAL)');
  sql(r, 'INSERT INTO t (x) VALUES (3.14159)');
  const rows = queryRows(r, 'SELECT ROUND(x, 2) AS rounded FROM t');
  assert(Math.abs(rows[0].rounded - 3.14) < 0.001);
});

test('CONCAT()', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT CONCAT(name, ' costs ', price) AS description FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].description, 'Widget costs 10');
});

test('|| string concatenation', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT name || '-' || price AS label FROM products WHERE name = 'Gadget'");
  assertEqual(rows[0].label, 'Gadget-25');
});

test('SUBSTR()', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT SUBSTR(name, 1, 3) AS short FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].short, 'Wid');
});

test('REPLACE()', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT REPLACE(name, 'get', 'gizmo') AS replaced FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].replaced, 'Widgizmo');
});

test('TRIM()', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (s TEXT)');
  sql(r, "INSERT INTO t (s) VALUES ('  hello  ')");
  const rows = queryRows(r, 'SELECT TRIM(s) AS trimmed FROM t');
  assertEqual(rows[0].trimmed, 'hello');
});

test('COALESCE()', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (a INTEGER, b INTEGER)');
  sql(r, 'INSERT INTO t (b) VALUES (42)');
  const rows = queryRows(r, 'SELECT COALESCE(a, b, 0) AS result FROM t');
  assertEqual(rows[0].result, 42);
});

test('IFNULL()', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (a INTEGER, b INTEGER)');
  sql(r, 'INSERT INTO t (b) VALUES (99)');
  const rows = queryRows(r, 'SELECT IFNULL(a, b) AS result FROM t');
  assertEqual(rows[0].result, 99);
});

test('NULLIF()', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (a INTEGER)');
  sql(r, 'INSERT INTO t (a) VALUES (5)');
  sql(r, 'INSERT INTO t (a) VALUES (0)');
  const rows = queryRows(r, 'SELECT a, NULLIF(a, 0) AS result FROM t ORDER BY a');
  assert(rows[0].result === null || rows[0].result === undefined); // 0 NULLIF 0
  assertEqual(rows[1].result, 5);
});

test('CAST()', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (x TEXT)');
  sql(r, "INSERT INTO t (x) VALUES ('42')");
  const rows = queryRows(r, 'SELECT CAST(x AS INTEGER) AS num FROM t');
  assertEqual(rows[0].num, 42);
});

test('Nested functions', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT UPPER(SUBSTR(name, 1, 3)) AS code FROM products WHERE name = 'Widget'");
  assertEqual(rows[0].code, 'WID');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CASE WHEN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('CASE WHEN basic', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT name, price, CASE WHEN price > 20 THEN 'expensive' WHEN price > 8 THEN 'medium' ELSE 'cheap' END AS tier FROM products ORDER BY price DESC");
  assertEqual(rows[0].tier, 'expensive'); // Gadget 25
  assertEqual(rows[1].tier, 'medium');    // Widget 10
  assertEqual(rows[2].tier, 'cheap');     // Doohickey 7
});

test('CASE WHEN without ELSE', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT name, CASE WHEN price > 20 THEN 'expensive' END AS tier FROM products WHERE name = 'Widget'");
  assert(rows[0].tier === null || rows[0].tier === undefined);
});

test('CASE WHEN with expression', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT name, CASE WHEN price * qty > 60 THEN 'high' ELSE 'low' END AS volume FROM products ORDER BY name");
  assertEqual(rows[0].volume, 'high'); // Doohickey 84
  assertEqual(rows[1].volume, 'high'); // Gadget 75
  assertEqual(rows[2].volume, 'low');  // Widget 50
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// UNION / UNION ALL
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('UNION ALL combines rows', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE a (x INTEGER)');
  sql(r, 'CREATE TABLE b (x INTEGER)');
  sql(r, 'INSERT INTO a (x) VALUES (1), (2)');
  sql(r, 'INSERT INTO b (x) VALUES (3), (4)');
  const rows = queryRows(r, 'SELECT x FROM a UNION ALL SELECT x FROM b');
  assertEqual(rows.length, 4);
});

test('UNION removes duplicates', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE a (x INTEGER)');
  sql(r, 'CREATE TABLE b (x INTEGER)');
  sql(r, 'INSERT INTO a (x) VALUES (1), (2), (3)');
  sql(r, 'INSERT INTO b (x) VALUES (2), (3), (4)');
  const rows = queryRows(r, 'SELECT x FROM a UNION SELECT x FROM b');
  assertEqual(rows.length, 4); // 1,2,3,4
});

test('UNION ALL keeps duplicates', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE a (x INTEGER)');
  sql(r, 'CREATE TABLE b (x INTEGER)');
  sql(r, 'INSERT INTO a (x) VALUES (1), (2)');
  sql(r, 'INSERT INTO b (x) VALUES (2), (3)');
  const rows = queryRows(r, 'SELECT x FROM a UNION ALL SELECT x FROM b');
  assertEqual(rows.length, 4); // 1,2,2,3
});

test('UNION with WHERE on both sides', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE a (x INTEGER, label TEXT)');
  sql(r, 'CREATE TABLE b (x INTEGER, label TEXT)');
  sql(r, "INSERT INTO a (x, label) VALUES (1, 'a'), (2, 'a'), (3, 'a')");
  sql(r, "INSERT INTO b (x, label) VALUES (4, 'b'), (5, 'b'), (6, 'b')");
  const rows = queryRows(r, "SELECT x FROM a WHERE x > 1 UNION ALL SELECT x FROM b WHERE x < 6");
  assertEqual(rows.length, 4); // 2,3,4,5
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// END-TO-END INTEGRATION
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

test('complex expression query', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, `
    SELECT
      UPPER(name) AS product,
      price * qty AS revenue,
      CASE WHEN price * qty > 60 THEN 'A' ELSE 'B' END AS grade
    FROM products
    WHERE price * qty > 40
    ORDER BY revenue DESC
  `);
  assertEqual(rows.length, 3);
  assertEqual(rows[0].product, 'DOOHICKEY');
  assertEqual(rows[0].revenue, 84);
  assertEqual(rows[0].grade, 'A');
});

test('function in WHERE and SELECT', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, "SELECT LOWER(name) AS n FROM products WHERE LENGTH(name) > 6");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].n, 'doohickey');
});

test('UNION with expressions', () => {
  const r = freshRunner();
  setupProducts(r);
  sql(r, 'CREATE TABLE deals (item TEXT, discount INTEGER)');
  sql(r, "INSERT INTO deals (item, discount) VALUES ('Widget', 2), ('Gadget', 5)");
  const rows = queryRows(r, `
    SELECT name AS item, price AS amount FROM products WHERE price > 8
    UNION ALL
    SELECT item, discount AS amount FROM deals
  `);
  assertEqual(rows.length, 4);
});

test('multiple expressions in single SELECT', () => {
  const r = freshRunner();
  setupProducts(r);
  const rows = queryRows(r, `
    SELECT name, price * qty AS revenue, price + qty AS combined, price - qty AS diff
    FROM products WHERE name = 'Widget'
  `);
  assertEqual(rows[0].revenue, 50);
  assertEqual(rows[0].combined, 15);
  assertEqual(rows[0].diff, 5);
});

report('phase11-expressions');
