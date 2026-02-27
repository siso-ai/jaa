/**
 * ICE Database — Phase 20 JS Test Suite
 * Expressions in INSERT, NULLS FIRST/LAST, Recursive CTEs.
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
section('Expressions in INSERT VALUES');

test('INSERT with arithmetic expression', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE calc (val INTEGER)');
  sql(r, 'INSERT INTO calc (val) VALUES (2 + 3)');
  const rows = queryRows(r, 'SELECT val FROM calc');
  assertEqual(rows[0].val, 5);
});

test('INSERT with function expression', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE data (name TEXT)');
  sql(r, "INSERT INTO data (name) VALUES (UPPER('hello'))");
  const rows = queryRows(r, 'SELECT name FROM data');
  assertEqual(rows[0].name, 'HELLO');
});

test('INSERT with mixed literals and expressions', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE items (name TEXT, price INTEGER)');
  sql(r, "INSERT INTO items (name, price) VALUES ('Widget', 10 * 3)");
  const rows = queryRows(r, 'SELECT name, price FROM items');
  assertEqual(rows[0].name, 'Widget');
  assertEqual(rows[0].price, 30);
});

test('INSERT with nested function', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE log (msg TEXT)');
  sql(r, "INSERT INTO log (msg) VALUES (LOWER(REPLACE('Hello World', 'World', 'Earth')))");
  const rows = queryRows(r, 'SELECT msg FROM log');
  assertEqual(rows[0].msg, 'hello earth');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('NULLS FIRST / NULLS LAST');

test('ORDER BY ASC NULLS FIRST', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (v INTEGER)');
  sql(r, 'INSERT INTO t (v) VALUES (3)');
  sql(r, 'INSERT INTO t (v) VALUES (NULL)');
  sql(r, 'INSERT INTO t (v) VALUES (1)');
  const rows = queryRows(r, 'SELECT v FROM t ORDER BY v ASC NULLS FIRST');
  assertEqual(rows[0].v, null);
  assertEqual(rows[1].v, 1);
  assertEqual(rows[2].v, 3);
});

test('ORDER BY ASC NULLS LAST', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (v INTEGER)');
  sql(r, 'INSERT INTO t (v) VALUES (3)');
  sql(r, 'INSERT INTO t (v) VALUES (NULL)');
  sql(r, 'INSERT INTO t (v) VALUES (1)');
  const rows = queryRows(r, 'SELECT v FROM t ORDER BY v ASC NULLS LAST');
  assertEqual(rows[0].v, 1);
  assertEqual(rows[1].v, 3);
  assertEqual(rows[2].v, null);
});

test('ORDER BY DESC NULLS FIRST', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (v INTEGER)');
  sql(r, 'INSERT INTO t (v) VALUES (3)');
  sql(r, 'INSERT INTO t (v) VALUES (NULL)');
  sql(r, 'INSERT INTO t (v) VALUES (1)');
  const rows = queryRows(r, 'SELECT v FROM t ORDER BY v DESC NULLS FIRST');
  assertEqual(rows[0].v, null);
  assertEqual(rows[1].v, 3);
  assertEqual(rows[2].v, 1);
});

test('ORDER BY DESC NULLS LAST', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE t (v INTEGER)');
  sql(r, 'INSERT INTO t (v) VALUES (3)');
  sql(r, 'INSERT INTO t (v) VALUES (NULL)');
  sql(r, 'INSERT INTO t (v) VALUES (1)');
  const rows = queryRows(r, 'SELECT v FROM t ORDER BY v DESC NULLS LAST');
  assertEqual(rows[0].v, 3);
  assertEqual(rows[1].v, 1);
  assertEqual(rows[2].v, null);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Recursive CTEs');

test('Simple counting recursive CTE', () => {
  const r = freshRunner();
  const rows = queryRows(r, 'WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 5) SELECT x FROM cnt');
  assertEqual(rows.length, 5);
  assertEqual(rows[0].x, 1);
  assertEqual(rows[4].x, 5);
});

test('Fibonacci recursive CTE', () => {
  const r = freshRunner();
  const rows = queryRows(r, 'WITH RECURSIVE fib(a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 50) SELECT a FROM fib');
  assertEqual(rows.length, 10);
  assertEqual(rows[0].a, 0);
  assertEqual(rows[1].a, 1);
  assertEqual(rows[5].a, 5);
  assertEqual(rows[9].a, 34);
});

test('Recursive CTE for hierarchical data', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE emp (id INTEGER, name TEXT, manager_id INTEGER)');
  sql(r, "INSERT INTO emp (id, name, manager_id) VALUES (1, 'CEO', NULL)");
  sql(r, "INSERT INTO emp (id, name, manager_id) VALUES (2, 'VP', 1)");
  sql(r, "INSERT INTO emp (id, name, manager_id) VALUES (3, 'Dir', 2)");
  sql(r, "INSERT INTO emp (id, name, manager_id) VALUES (4, 'Mgr', 3)");
  const rows = queryRows(r, "WITH RECURSIVE chain(id, name, lvl) AS (SELECT id, name, 0 FROM emp WHERE name = 'CEO' UNION ALL SELECT e.id, e.name, c.lvl + 1 FROM emp e JOIN chain c ON e.manager_id = c.id) SELECT name, lvl FROM chain ORDER BY lvl");
  assertEqual(rows.length, 4);
  assertEqual(rows[0].name, 'CEO');
  assertEqual(rows[0].lvl, 0);
  assertEqual(rows[3].name, 'Mgr');
  assertEqual(rows[3].lvl, 3);
});

test('Recursive CTE with string accumulation', () => {
  const r = freshRunner();
  const rows = queryRows(r, "WITH RECURSIVE stars(n, s) AS (SELECT 1, '*' UNION ALL SELECT n + 1, s || '*' FROM stars WHERE n < 4) SELECT n, s FROM stars");
  assertEqual(rows.length, 4);
  assertEqual(rows[0].s, '*');
  assertEqual(rows[3].s, '****');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Integration');

test('Recursive CTE + LIMIT', () => {
  const r = freshRunner();
  const rows = queryRows(r, 'WITH RECURSIVE cnt(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM cnt WHERE x < 100) SELECT x FROM cnt LIMIT 10');
  assertEqual(rows.length, 10);
  assertEqual(rows[9].x, 10);
});

test('INSERT expression + NULLS ordering', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE scores (name TEXT, score INTEGER)');
  sql(r, "INSERT INTO scores (name, score) VALUES ('Alice', 50 + 40)");
  sql(r, "INSERT INTO scores (name, score) VALUES ('Bob', NULL)");
  sql(r, "INSERT INTO scores (name, score) VALUES ('Carol', 20 * 3)");
  const rows = queryRows(r, 'SELECT name, score FROM scores ORDER BY score DESC NULLS LAST');
  assertEqual(rows[0].name, 'Alice');
  assertEqual(rows[0].score, 90);
  assertEqual(rows[1].name, 'Carol');
  assertEqual(rows[2].score, null);
});

report('phase20-insert-expr-nulls-order-recursive-cte');
