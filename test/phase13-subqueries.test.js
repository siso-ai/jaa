/**
 * ICE Database — Phase 13 JS Test Suite
 * Subqueries (IN, EXISTS, scalar), Table Aliases.
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
  sql(r, 'CREATE TABLE departments (name TEXT, budget INTEGER)');
  sql(r, "INSERT INTO departments (name, budget) VALUES ('Engineering', 500000)");
  sql(r, "INSERT INTO departments (name, budget) VALUES ('Marketing', 200000)");
  sql(r, "INSERT INTO departments (name, budget) VALUES ('Sales', 300000)");

  sql(r, 'CREATE TABLE employees (name TEXT, dept TEXT, salary INTEGER)');
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Alice', 'Engineering', 120000)");
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Bob', 'Marketing', 80000)");
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Charlie', 'Engineering', 110000)");
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Diana', 'Sales', 90000)");
  sql(r, "INSERT INTO employees (name, dept, salary) VALUES ('Eve', 'Marketing', 75000)");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// IN SUBQUERY
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('IN subquery');

test('WHERE col IN (SELECT ...)', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name FROM employees WHERE dept IN (SELECT name FROM departments WHERE budget > 250000)");
  assertEqual(rows.length, 3); // Engineering (Alice, Charlie) + Sales (Diana)
});

test('WHERE col NOT IN (SELECT ...)', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name FROM employees WHERE dept NOT IN (SELECT name FROM departments WHERE budget > 250000)");
  assertEqual(rows.length, 2); // Marketing (Bob, Eve)
});

test('IN subquery returns single column', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name FROM departments WHERE name IN (SELECT dept FROM employees WHERE salary > 100000)");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Engineering');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// EXISTS SUBQUERY
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('EXISTS subquery');

test('WHERE EXISTS (SELECT ...)', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name FROM departments WHERE EXISTS (SELECT name FROM employees WHERE salary > 100000)");
  assertEqual(rows.length, 3);
});

test('WHERE NOT EXISTS (SELECT ...)', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name FROM departments WHERE NOT EXISTS (SELECT name FROM employees WHERE salary > 200000)");
  assertEqual(rows.length, 3);
});

test('EXISTS with empty result', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name FROM departments WHERE EXISTS (SELECT name FROM employees WHERE salary > 999999)");
  assertEqual(rows.length, 0);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SCALAR SUBQUERY IN SELECT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Scalar subquery in SELECT');

test('scalar subquery in SELECT list', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name, (SELECT MAX(salary) FROM employees) AS max_sal FROM departments WHERE name = 'Engineering'");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].max_sal, 120000);
});

test('scalar subquery returns null when empty', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name, (SELECT salary FROM employees WHERE salary > 999999) AS big_sal FROM departments WHERE name = 'Engineering'");
  assertEqual(rows.length, 1);
  assert(rows[0].big_sal === null || rows[0].big_sal === undefined);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// TABLE ALIASES
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Table aliases');

test('FROM table alias', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT e.name FROM employees e WHERE e.salary > 100000");
  assertEqual(rows.length, 2);
});

test('FROM table AS alias', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT e.name FROM employees AS e WHERE e.salary > 100000");
  assertEqual(rows.length, 2);
});

test('JOIN with aliases', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT e.name, d.budget FROM employees e JOIN departments d ON e.dept = d.name WHERE d.budget > 250000 ORDER BY e.name");
  assertEqual(rows.length, 3);
  assertEqual(rows[0].name, 'Alice');
  assertEqual(rows[0].budget, 500000);
});

test('alias in ORDER BY', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT e.name, e.salary FROM employees e ORDER BY e.salary DESC");
  assertEqual(rows[0].name, 'Alice');
  assertEqual(rows[0].salary, 120000);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// COMBINED FEATURES
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Combined features');

test('IN subquery with expression', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name, salary FROM employees WHERE salary IN (SELECT MAX(salary) FROM employees)");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Alice');
});

test('nested subqueries', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT name FROM employees WHERE dept IN (SELECT name FROM departments WHERE budget > (SELECT MIN(budget) FROM departments))");
  assertEqual(rows.length, 3);
});

test('scalar subquery + table alias + WHERE', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT e.name, e.salary, (SELECT MAX(salary) FROM employees) AS top_sal FROM employees e WHERE e.salary > 100000");
  assertEqual(rows.length, 2);
  assertEqual(rows[0].top_sal, 120000);
});

test('EXISTS + alias', () => {
  const r = freshRunner();
  setupData(r);
  const rows = queryRows(r, "SELECT d.name FROM departments d WHERE EXISTS (SELECT name FROM employees WHERE salary > 100000)");
  assertEqual(rows.length, 3);
});

report('phase13-subqueries');
