/**
 * ICE Database — Phase 19 JS Test Suite
 * IIF, IFNULL, UNION+ORDER BY+LIMIT, UPDATE FROM, date/time functions.
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

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('IIF function');

test('IIF true branch', () => {
  const r = freshRunner();
  const rows = queryRows(r, 'SELECT IIF(1 > 0, 10, 20) AS val');
  assertEqual(rows[0].val, 10);
});

test('IIF false branch', () => {
  const r = freshRunner();
  const rows = queryRows(r, 'SELECT IIF(1 = 2, 10, 20) AS val');
  assertEqual(rows[0].val, 20);
});

test('IIF with column values', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE scores (name TEXT, score INTEGER)');
  sql(r, "INSERT INTO scores (name, score) VALUES ('Alice', 90)");
  sql(r, "INSERT INTO scores (name, score) VALUES ('Bob', 60)");
  const rows = queryRows(r, "SELECT name, IIF(score >= 70, 'pass', 'fail') AS result FROM scores ORDER BY name");
  assertEqual(rows[0].result, 'pass');
  assertEqual(rows[1].result, 'fail');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('IFNULL function');

test('IFNULL returns first non-null', () => {
  const r = freshRunner();
  const rows = queryRows(r, 'SELECT IFNULL(NULL, 42) AS val');
  assertEqual(rows[0].val, 42);
});

test('IFNULL returns first arg if not null', () => {
  const r = freshRunner();
  const rows = queryRows(r, 'SELECT IFNULL(10, 42) AS val');
  assertEqual(rows[0].val, 10);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('UNION with ORDER BY and LIMIT');

test('UNION ALL + ORDER BY', () => {
  const r = freshRunner();
  const rows = queryRows(r, 'SELECT 3 AS n UNION ALL SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n');
  assertEqual(rows.length, 3);
  assertEqual(rows[0].n, 1);
  assertEqual(rows[1].n, 2);
  assertEqual(rows[2].n, 3);
});

test('UNION ALL + ORDER BY + LIMIT', () => {
  const r = freshRunner();
  const rows = queryRows(r, 'SELECT 3 AS n UNION ALL SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n LIMIT 2');
  assertEqual(rows.length, 2);
  assertEqual(rows[0].n, 1);
  assertEqual(rows[1].n, 2);
});

test('UNION + ORDER BY DESC', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE a (x INTEGER)');
  sql(r, 'INSERT INTO a (x) VALUES (1)');
  sql(r, 'INSERT INTO a (x) VALUES (3)');
  sql(r, 'CREATE TABLE b (x INTEGER)');
  sql(r, 'INSERT INTO b (x) VALUES (2)');
  sql(r, 'INSERT INTO b (x) VALUES (4)');
  const rows = queryRows(r, 'SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x DESC');
  assertEqual(rows.length, 4);
  assertEqual(rows[0].x, 4);
  assertEqual(rows[3].x, 1);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('UPDATE FROM (join update)');

test('UPDATE FROM basic join', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE emp (name TEXT, salary INTEGER, dept TEXT)');
  sql(r, "INSERT INTO emp (name, salary, dept) VALUES ('Alice', 100, 'Eng')");
  sql(r, "INSERT INTO emp (name, salary, dept) VALUES ('Bob', 200, 'Sales')");
  sql(r, 'CREATE TABLE bonuses (dept TEXT, bonus INTEGER)');
  sql(r, "INSERT INTO bonuses (dept, bonus) VALUES ('Eng', 50)");
  sql(r, "INSERT INTO bonuses (dept, bonus) VALUES ('Sales', 100)");
  sql(r, 'UPDATE emp SET salary = salary + bonuses.bonus FROM bonuses WHERE emp.dept = bonuses.dept');
  const rows = queryRows(r, 'SELECT name, salary FROM emp ORDER BY name');
  assertEqual(rows[0].salary, 150);
  assertEqual(rows[1].salary, 300);
});

test('UPDATE FROM partial match', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE products (name TEXT, price INTEGER, category TEXT)');
  sql(r, "INSERT INTO products (name, price, category) VALUES ('Widget', 10, 'A')");
  sql(r, "INSERT INTO products (name, price, category) VALUES ('Gadget', 20, 'B')");
  sql(r, "INSERT INTO products (name, price, category) VALUES ('Bolt', 5, 'C')");
  sql(r, 'CREATE TABLE price_changes (category TEXT, adjustment INTEGER)');
  sql(r, "INSERT INTO price_changes (category, adjustment) VALUES ('A', 5)");
  sql(r, "INSERT INTO price_changes (category, adjustment) VALUES ('B', -3)");
  sql(r, 'UPDATE products SET price = price + price_changes.adjustment FROM price_changes WHERE products.category = price_changes.category');
  const rows = queryRows(r, 'SELECT name, price FROM products ORDER BY name');
  assertEqual(rows[0].price, 5);   // Bolt: unchanged
  assertEqual(rows[1].price, 17);  // Gadget: 20-3
  assertEqual(rows[2].price, 15);  // Widget: 10+5
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Date/Time functions');

test('DATE returns valid date format', () => {
  const r = freshRunner();
  const rows = queryRows(r, "SELECT DATE('now') AS d");
  assert(/^\d{4}-\d{2}-\d{2}$/.test(rows[0].d), 'Date format YYYY-MM-DD');
});

test('TIME returns valid time format', () => {
  const r = freshRunner();
  const rows = queryRows(r, "SELECT TIME('now') AS t");
  assert(/^\d{2}:\d{2}:\d{2}$/.test(rows[0].t), 'Time format HH:MM:SS');
});

test('DATETIME returns valid datetime format', () => {
  const r = freshRunner();
  const rows = queryRows(r, "SELECT DATETIME('now') AS dt");
  assert(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(rows[0].dt), 'DateTime format');
});

test('CURRENT_DATE returns date', () => {
  const r = freshRunner();
  const rows = queryRows(r, "SELECT CURRENT_DATE() AS d");
  assert(/^\d{4}-\d{2}-\d{2}$/.test(rows[0].d), 'CURRENT_DATE format');
});

test('DATE with specific value', () => {
  const r = freshRunner();
  const rows = queryRows(r, "SELECT DATE('2024-06-15 14:30:00') AS d");
  assertEqual(rows[0].d, '2024-06-15');
});

test('STRFTIME format', () => {
  const r = freshRunner();
  const rows = queryRows(r, "SELECT STRFTIME('%Y', '2024-06-15') AS y");
  assertEqual(rows[0].y, '2024');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Integration');

test('IIF in SELECT with WHERE', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE orders (item TEXT, amount INTEGER)');
  sql(r, "INSERT INTO orders (item, amount) VALUES ('A', 100)");
  sql(r, "INSERT INTO orders (item, amount) VALUES ('B', 200)");
  sql(r, "INSERT INTO orders (item, amount) VALUES ('C', 50)");
  const rows = queryRows(r, "SELECT item, IIF(amount >= 100, 'big', 'small') AS size FROM orders ORDER BY item");
  assertEqual(rows[0].size, 'big');
  assertEqual(rows[1].size, 'big');
  assertEqual(rows[2].size, 'small');
});

test('UNION ALL + ORDER BY from tables', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE cats (name TEXT, age INTEGER)');
  sql(r, "INSERT INTO cats (name, age) VALUES ('Whiskers', 5)");
  sql(r, "INSERT INTO cats (name, age) VALUES ('Mittens', 3)");
  sql(r, 'CREATE TABLE dogs (name TEXT, age INTEGER)');
  sql(r, "INSERT INTO dogs (name, age) VALUES ('Buddy', 7)");
  sql(r, "INSERT INTO dogs (name, age) VALUES ('Rex', 2)");
  const rows = queryRows(r, 'SELECT name, age FROM cats UNION ALL SELECT name, age FROM dogs ORDER BY age');
  assertEqual(rows.length, 4);
  assertEqual(rows[0].name, 'Rex');
  assertEqual(rows[3].name, 'Buddy');
});

test('UPDATE FROM + RETURNING', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE emp (name TEXT, salary INTEGER, dept TEXT)');
  sql(r, "INSERT INTO emp (name, salary, dept) VALUES ('Alice', 100, 'Eng')");
  sql(r, "INSERT INTO emp (name, salary, dept) VALUES ('Bob', 200, 'Sales')");
  sql(r, 'CREATE TABLE raises (dept TEXT, amount INTEGER)');
  sql(r, "INSERT INTO raises (dept, amount) VALUES ('Eng', 25)");
  const rows = queryRows(r, 'UPDATE emp SET salary = salary + raises.amount FROM raises WHERE emp.dept = raises.dept RETURNING name, salary');
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Alice');
  assertEqual(rows[0].salary, 125);
});

report('phase19-iif-updatefrom-union-order-datetime');
