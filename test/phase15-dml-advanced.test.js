/**
 * ICE Database — Phase 15 JS Test Suite
 * UPSERT (ON CONFLICT), RETURNING, TRUNCATE TABLE, Multi-JOIN.
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
section('TRUNCATE TABLE');

test('TRUNCATE TABLE removes all rows', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE items (name TEXT, price INTEGER)');
  sql(r, "INSERT INTO items (name, price) VALUES ('A', 10)");
  sql(r, "INSERT INTO items (name, price) VALUES ('B', 20)");
  sql(r, "INSERT INTO items (name, price) VALUES ('C', 30)");
  assertEqual(queryRows(r, 'SELECT * FROM items').length, 3);
  sql(r, 'TRUNCATE TABLE items');
  assertEqual(queryRows(r, 'SELECT * FROM items').length, 0);
});

test('TRUNCATE without TABLE keyword', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE items (name TEXT)');
  sql(r, "INSERT INTO items (name) VALUES ('A')");
  sql(r, 'TRUNCATE items');
  assertEqual(queryRows(r, 'SELECT * FROM items').length, 0);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('UPSERT — ON CONFLICT DO NOTHING');

test('no conflict — inserts normally', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE users (email TEXT, name TEXT)');
  sql(r, "INSERT INTO users (email, name) VALUES ('a@test.com', 'Alice')");
  sql(r, "INSERT INTO users (email, name) VALUES ('b@test.com', 'Bob') ON CONFLICT (email) DO NOTHING");
  assertEqual(queryRows(r, 'SELECT * FROM users ORDER BY email').length, 2);
});

test('with conflict — skips insert', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE users (email TEXT, name TEXT)');
  sql(r, "INSERT INTO users (email, name) VALUES ('a@test.com', 'Alice')");
  sql(r, "INSERT INTO users (email, name) VALUES ('a@test.com', 'Alice2') ON CONFLICT (email) DO NOTHING");
  const rows = queryRows(r, 'SELECT * FROM users');
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Alice');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('UPSERT — ON CONFLICT DO UPDATE');

test('updates existing row on conflict', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE users (email TEXT, name TEXT, visits INTEGER)');
  sql(r, "INSERT INTO users (email, name, visits) VALUES ('a@test.com', 'Alice', 1)");
  sql(r, "INSERT INTO users (email, name, visits) VALUES ('a@test.com', 'Alice', 1) ON CONFLICT (email) DO UPDATE SET visits = visits + 1");
  const rows = queryRows(r, 'SELECT * FROM users');
  assertEqual(rows.length, 1);
  assertEqual(rows[0].visits, 2);
});

test('updates multiple columns', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE users (email TEXT, name TEXT, visits INTEGER)');
  sql(r, "INSERT INTO users (email, name, visits) VALUES ('a@test.com', 'Alice', 1)");
  sql(r, "INSERT INTO users (email, name, visits) VALUES ('a@test.com', 'Alice Updated', 99) ON CONFLICT (email) DO UPDATE SET name = 'Alice V2', visits = visits + 1");
  const rows = queryRows(r, 'SELECT * FROM users');
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Alice V2');
  assertEqual(rows[0].visits, 2);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('RETURNING — INSERT');

test('INSERT RETURNING *', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE items (name TEXT, price INTEGER)');
  const rows = queryRows(r, "INSERT INTO items (name, price) VALUES ('Widget', 25) RETURNING *");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Widget');
  assertEqual(rows[0].price, 25);
  assert(rows[0].id !== undefined);
});

test('INSERT RETURNING specific columns', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE items (name TEXT, price INTEGER)');
  const rows = queryRows(r, "INSERT INTO items (name, price) VALUES ('Widget', 25) RETURNING id, name");
  assertEqual(rows.length, 1);
  assert(rows[0].id !== undefined);
  assertEqual(rows[0].name, 'Widget');
  assert(rows[0].price === undefined || rows[0].price === null);
});

test('UPSERT + RETURNING', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE users (email TEXT, name TEXT)');
  sql(r, "INSERT INTO users (email, name) VALUES ('a@test.com', 'Alice')");
  const rows = queryRows(r, "INSERT INTO users (email, name) VALUES ('a@test.com', 'Bob') ON CONFLICT (email) DO UPDATE SET name = 'Updated' RETURNING name");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Updated');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('RETURNING — UPDATE');

test('UPDATE RETURNING *', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE items (name TEXT, price INTEGER)');
  sql(r, "INSERT INTO items (name, price) VALUES ('Widget', 25)");
  sql(r, "INSERT INTO items (name, price) VALUES ('Gadget', 50)");
  const rows = queryRows(r, "UPDATE items SET price = price + 10 WHERE name = 'Widget' RETURNING *");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Widget');
  assertEqual(rows[0].price, 35);
});

test('UPDATE RETURNING specific columns', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE items (name TEXT, price INTEGER)');
  sql(r, "INSERT INTO items (name, price) VALUES ('Widget', 25)");
  const rows = queryRows(r, "UPDATE items SET price = 99 RETURNING name, price");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].price, 99);
  assert(rows[0].id === undefined || rows[0].id === null);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('RETURNING — DELETE');

test('DELETE RETURNING *', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE items (name TEXT, price INTEGER)');
  sql(r, "INSERT INTO items (name, price) VALUES ('Widget', 25)");
  sql(r, "INSERT INTO items (name, price) VALUES ('Gadget', 50)");
  const rows = queryRows(r, "DELETE FROM items WHERE name = 'Widget' RETURNING *");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Widget');
  assertEqual(queryRows(r, 'SELECT * FROM items').length, 1);
});

test('DELETE RETURNING specific columns', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE items (name TEXT, price INTEGER)');
  sql(r, "INSERT INTO items (name, price) VALUES ('Widget', 25)");
  const rows = queryRows(r, "DELETE FROM items WHERE name = 'Widget' RETURNING id, name");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Widget');
  assert(rows[0].price === undefined || rows[0].price === null);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Multi-JOIN');

test('three-table JOIN', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE departments (name TEXT)');
  sql(r, "INSERT INTO departments (name) VALUES ('Engineering')");
  sql(r, "INSERT INTO departments (name) VALUES ('Marketing')");
  sql(r, 'CREATE TABLE employees (name TEXT, dept TEXT)');
  sql(r, "INSERT INTO employees (name, dept) VALUES ('Alice', 'Engineering')");
  sql(r, "INSERT INTO employees (name, dept) VALUES ('Bob', 'Marketing')");
  sql(r, 'CREATE TABLE projects (title TEXT, dept TEXT)');
  sql(r, "INSERT INTO projects (title, dept) VALUES ('Project X', 'Engineering')");
  sql(r, "INSERT INTO projects (title, dept) VALUES ('Campaign A', 'Marketing')");

  const rows = queryRows(r, "SELECT e.name, p.title FROM employees e JOIN departments d ON e.dept = d.name JOIN projects p ON p.dept = d.name ORDER BY e.name");
  assertEqual(rows.length, 2);
  assertEqual(rows[0].name, 'Alice');
  assertEqual(rows[0].title, 'Project X');
  assertEqual(rows[1].name, 'Bob');
  assertEqual(rows[1].title, 'Campaign A');
});

test('three-table JOIN with filter', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE a (val TEXT)');
  sql(r, "INSERT INTO a (val) VALUES ('x')");
  sql(r, "INSERT INTO a (val) VALUES ('y')");
  sql(r, 'CREATE TABLE b (val TEXT, extra INTEGER)');
  sql(r, "INSERT INTO b (val, extra) VALUES ('x', 10)");
  sql(r, "INSERT INTO b (val, extra) VALUES ('y', 20)");
  sql(r, 'CREATE TABLE c (val TEXT, info TEXT)');
  sql(r, "INSERT INTO c (val, info) VALUES ('x', 'hello')");
  sql(r, "INSERT INTO c (val, info) VALUES ('y', 'world')");

  const rows = queryRows(r, "SELECT a.val, b.extra, c.info FROM a JOIN b ON a.val = b.val JOIN c ON a.val = c.val WHERE b.extra > 15");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].val, 'y');
  assertEqual(rows[0].extra, 20);
  assertEqual(rows[0].info, 'world');
});

test('LEFT JOIN + INNER JOIN chained', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE orders (id INTEGER, customer TEXT)');
  sql(r, "INSERT INTO orders (id, customer) VALUES (1, 'Alice')");
  sql(r, "INSERT INTO orders (id, customer) VALUES (2, 'Bob')");
  sql(r, 'CREATE TABLE items (order_id INTEGER, product TEXT)');
  sql(r, "INSERT INTO items (order_id, product) VALUES (1, 'Widget')");
  sql(r, 'CREATE TABLE reviews (product TEXT, rating INTEGER)');
  sql(r, "INSERT INTO reviews (product, rating) VALUES ('Widget', 5)");

  const rows = queryRows(r, "SELECT o.customer, i.product, r.rating FROM orders o LEFT JOIN items i ON o.id = i.order_id LEFT JOIN reviews r ON i.product = r.product ORDER BY o.customer");
  assertEqual(rows.length, 2);
  assertEqual(rows[0].customer, 'Alice');
  assertEqual(rows[0].product, 'Widget');
  assertEqual(rows[0].rating, 5);
  assertEqual(rows[1].customer, 'Bob');
  assertEqual(rows[1].product, null);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Integration');

test('UPSERT + RETURNING + expression', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE counters (name TEXT, value INTEGER)');
  sql(r, "INSERT INTO counters (name, value) VALUES ('hits', 0)");
  const rows = queryRows(r, "INSERT INTO counters (name, value) VALUES ('hits', 0) ON CONFLICT (name) DO UPDATE SET value = value + 1 RETURNING name, value");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'hits');
  assertEqual(rows[0].value, 1);
});

test('CTE + multi-JOIN', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE departments (name TEXT, budget INTEGER)');
  sql(r, "INSERT INTO departments (name, budget) VALUES ('Engineering', 500000)");
  sql(r, 'CREATE TABLE employees (name TEXT, dept TEXT, salary INTEGER)');
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Alice', 'Engineering', 120000)");
  sql(r, 'CREATE TABLE projects (title TEXT, dept TEXT)');
  sql(r, "INSERT INTO projects (title, dept) VALUES ('Project X', 'Engineering')");

  const rows = queryRows(r, "WITH big_depts AS (SELECT name FROM departments WHERE budget > 300000) SELECT e.name, p.title FROM employees e JOIN big_depts bd ON e.dept = bd.name JOIN projects p ON p.dept = bd.name");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Alice');
  assertEqual(rows[0].title, 'Project X');
});

report('phase15-dml-advanced');
