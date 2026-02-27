/**
 * ICE Database — Phase 14 JS Test Suite
 * CTEs, Window Functions, Derived Tables, COUNT DISTINCT, GROUP_CONCAT.
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

function setupData(r) {
  sql(r, 'CREATE TABLE employees (name TEXT, dept TEXT, salary INTEGER)');
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Alice', 'Engineering', 120000)");
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Bob', 'Marketing', 80000)");
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Charlie', 'Engineering', 110000)");
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Diana', 'Sales', 90000)");
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Eve', 'Marketing', 75000)");
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Frank', 'Engineering', 130000)");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Common Table Expressions');

test('simple CTE', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "WITH eng AS (SELECT name, salary FROM employees WHERE dept = 'Engineering') SELECT name FROM eng WHERE salary > 115000");
  assertEqual(rows.length, 2);
});

test('CTE with aggregation', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "WITH dept_avg AS (SELECT dept, AVG(salary) AS avg_sal FROM employees GROUP BY dept) SELECT dept, avg_sal FROM dept_avg WHERE avg_sal > 85000");
  assertEqual(rows.length, 2);
});

test('multiple CTEs', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "WITH high AS (SELECT name FROM employees WHERE salary > 100000), low AS (SELECT name FROM employees WHERE salary < 80000) SELECT name FROM high");
  assertEqual(rows.length, 3);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Derived tables');

test('basic derived table', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT t.name FROM (SELECT name, salary FROM employees WHERE salary > 100000) AS t ORDER BY t.name");
  assertEqual(rows.length, 3);
  assertEqual(rows[0].name, 'Alice');
});

test('derived table with aggregation', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT d.dept FROM (SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY dept) AS d WHERE d.cnt > 1 ORDER BY d.dept");
  assertEqual(rows.length, 2);
  assertEqual(rows[0].dept, 'Engineering');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Window functions — ranking');

test('ROW_NUMBER() OVER (ORDER BY ...)', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM employees");
  assertEqual(rows.length, 6);
  const frank = rows.find(r => r.name === 'Frank');
  assertEqual(frank.rn, 1);
});

test('ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM employees");
  const frank = rows.find(r => r.name === 'Frank');
  assertEqual(frank.rn, 1);
  const alice = rows.find(r => r.name === 'Alice');
  assertEqual(alice.rn, 2);
});

test('RANK() with ties', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE scores (name TEXT, score INTEGER)');
  sql(r, "INSERT INTO scores (name, score) VALUES ('A', 100)");
  sql(r, "INSERT INTO scores (name, score) VALUES ('B', 90)");
  sql(r, "INSERT INTO scores (name, score) VALUES ('C', 90)");
  sql(r, "INSERT INTO scores (name, score) VALUES ('D', 80)");
  const rows = queryRows(r, "SELECT name, RANK() OVER (ORDER BY score DESC) AS rnk FROM scores");
  assertEqual(rows.find(r => r.name === 'A').rnk, 1);
  assertEqual(rows.find(r => r.name === 'B').rnk, 2);
  assertEqual(rows.find(r => r.name === 'C').rnk, 2);
  assertEqual(rows.find(r => r.name === 'D').rnk, 4);
});

test('DENSE_RANK() with ties', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE scores (name TEXT, score INTEGER)');
  sql(r, "INSERT INTO scores (name, score) VALUES ('A', 100)");
  sql(r, "INSERT INTO scores (name, score) VALUES ('B', 90)");
  sql(r, "INSERT INTO scores (name, score) VALUES ('C', 90)");
  sql(r, "INSERT INTO scores (name, score) VALUES ('D', 80)");
  const rows = queryRows(r, "SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) AS rnk FROM scores");
  assertEqual(rows.find(r => r.name === 'D').rnk, 3);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Window functions — aggregates');

test('SUM() OVER (PARTITION BY ...)', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name, dept, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM employees WHERE dept = 'Engineering'");
  assertEqual(rows.length, 3);
  assertEqual(rows[0].dept_total, 360000);
});

test('COUNT() OVER ()', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name, COUNT(*) OVER () AS total FROM employees");
  assertEqual(rows.length, 6);
  assertEqual(rows[0].total, 6);
});

test('AVG() OVER (PARTITION BY ...)', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name, dept, AVG(salary) OVER (PARTITION BY dept) AS dept_avg FROM employees WHERE dept = 'Marketing'");
  assertEqual(rows.length, 2);
  assertEqual(rows[0].dept_avg, 77500);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('COUNT DISTINCT');

test('COUNT(DISTINCT col)', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT COUNT(DISTINCT dept) AS dept_count FROM employees");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].dept_count, 3);
});

test('COUNT(DISTINCT col) with GROUP BY', () => {
  const r = freshRunner();
  setupData(r);
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Grace', 'Engineering', 120000)");
  const rows = queryRows(r, "SELECT dept, COUNT(DISTINCT salary) AS unique_sals FROM employees GROUP BY dept ORDER BY dept");
  const eng = rows.find(r => r.dept === 'Engineering');
  assertEqual(eng.unique_sals, 3);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('GROUP_CONCAT');

test('GROUP_CONCAT basic', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT dept, GROUP_CONCAT(name) AS names FROM employees GROUP BY dept ORDER BY dept");
  assertEqual(rows.length, 3);
  const eng = rows.find(r => r.dept === 'Engineering');
  const names = eng.names.split(',');
  assertEqual(names.length, 3);
});

test('GROUP_CONCAT with SEPARATOR', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT dept, GROUP_CONCAT(name SEPARATOR ' | ') AS names FROM employees WHERE dept = 'Marketing' GROUP BY dept");
  assertEqual(rows.length, 1);
  assert(rows[0].names.includes(' | '));
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Integration');

test('CTE + window function', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "WITH ranked AS (SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM employees) SELECT name, salary FROM ranked WHERE rn <= 3");
  assertEqual(rows.length, 3);
});

test('derived table + window function', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT t.name FROM (SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM employees) AS t WHERE t.rn = 1");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Frank');
});

report('phase14-advanced');
