import { test, assert, assertEqual, report } from './runner.js';
import { Runner } from '../src/resolution/Runner.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { MemoryRefs } from '../src/persistence/Refs.js';
import { registerDatabaseGates } from '../src/gates/database/register.js';
import { Event } from '../src/core/Event.js';

let store, refs, runner;

function freshWithData() {
  store = new MemoryStore();
  refs = new MemoryRefs();
  runner = new Runner({ store, refs });
  registerDatabaseGates(runner);

  runner.emit(new Event('create_table_execute', {
    table: 'users',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'text' },
      { name: 'age', type: 'integer' },
      { name: 'dept', type: 'text' }
    ]
  }));

  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Alice', age: 30, dept: 'eng' } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Bob', age: 25, dept: 'sales' } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Carol', age: 35, dept: 'eng' } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Dave', age: 25, dept: 'eng' } }));
  runner.emit(new Event('insert_execute', { table: 'users', row: { name: 'Eve', age: 30, dept: 'sales' } }));
}

// ── table scan ──────────────────────────────────

test('Query: table scan returns all rows', () => {
  freshWithData();
  runner.emit(new Event('table_scan', { table: 'users' }));
  const result = runner.sample().pending.find(e => e.type === 'scan_result');
  assert(result !== undefined);
  assertEqual(result.data.rows.length, 5);
});

// ── filter ──────────────────────────────────────

test('Query: filter gate works standalone', () => {
  freshWithData();
  runner.emit(new Event('filter', {
    rows: [
      { id: 1, name: 'Alice', age: 30 },
      { id: 2, name: 'Bob', age: 25 },
    ],
    where: { column: 'age', op: '>', value: 28 }
  }));
  const result = runner.sample().pending.find(e => e.type === 'filter_result');
  assertEqual(result.data.rows.length, 1);
  assertEqual(result.data.rows[0].name, 'Alice');
});

// ── project ─────────────────────────────────────

test('Query: projection gate selects columns', () => {
  freshWithData();
  runner.emit(new Event('project', {
    rows: [
      { id: 1, name: 'Alice', age: 30, dept: 'eng' },
    ],
    columns: ['name', 'age']
  }));
  const result = runner.sample().pending.find(e => e.type === 'project_result');
  assertEqual(Object.keys(result.data.rows[0]).length, 2);
  assertEqual(result.data.rows[0].name, 'Alice');
});

// ── order_by ────────────────────────────────────

test('Query: order_by gate sorts rows', () => {
  freshWithData();
  runner.emit(new Event('order_by', {
    rows: [
      { id: 1, name: 'Carol', age: 35 },
      { id: 2, name: 'Alice', age: 30 },
      { id: 3, name: 'Bob', age: 25 },
    ],
    order: [{ column: 'age', direction: 'asc' }]
  }));
  const result = runner.sample().pending.find(e => e.type === 'ordered_result');
  assertEqual(result.data.rows[0].name, 'Bob');
  assertEqual(result.data.rows[2].name, 'Carol');
});

// ── limit ───────────────────────────────────────

test('Query: limit gate restricts rows', () => {
  freshWithData();
  runner.emit(new Event('limit', {
    rows: [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }],
    limit: 2,
    offset: 1
  }));
  const result = runner.sample().pending.find(e => e.type === 'limited_result');
  assertEqual(result.data.rows.length, 2);
  assertEqual(result.data.rows[0].id, 2);
});

// ── distinct ────────────────────────────────────

test('Query: distinct gate deduplicates', () => {
  freshWithData();
  runner.emit(new Event('distinct', {
    rows: [
      { dept: 'eng' },
      { dept: 'sales' },
      { dept: 'eng' },
    ],
    columns: ['dept']
  }));
  const result = runner.sample().pending.find(e => e.type === 'distinct_result');
  assertEqual(result.data.rows.length, 2);
});

// ── aggregate ───────────────────────────────────

test('Query: aggregate gate with GROUP BY', () => {
  freshWithData();
  runner.emit(new Event('aggregate', {
    rows: [
      { dept: 'eng', age: 30 },
      { dept: 'eng', age: 35 },
      { dept: 'sales', age: 25 },
    ],
    aggregates: [{ fn: 'COUNT', column: '*', alias: 'count' }],
    groupBy: ['dept']
  }));
  const result = runner.sample().pending.find(e => e.type === 'aggregate_result');
  assertEqual(result.data.rows.length, 2);
  const eng = result.data.rows.find(r => r.dept === 'eng');
  assertEqual(eng.count, 2);
});

// ── join via Runner ─────────────────────────────

test('Query: join gate reads from two tables', () => {
  freshWithData();

  // Create departments table
  runner.emit(new Event('create_table_execute', {
    table: 'depts',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'dept_name', type: 'text' },
      { name: 'dept_code', type: 'text' }
    ]
  }));
  runner.emit(new Event('insert_execute', { table: 'depts', row: { dept_name: 'Engineering', dept_code: 'eng' } }));
  runner.emit(new Event('insert_execute', { table: 'depts', row: { dept_name: 'Sales', dept_code: 'sales' } }));

  runner.emit(new Event('join', {
    left: { table: 'users' },
    right: { table: 'depts' },
    on: { left: 'dept', right: 'dept_code' },
    type: 'inner'
  }));

  const result = runner.sample().pending.find(e => e.type === 'join_result');
  assert(result !== undefined, 'join_result emitted');
  assertEqual(result.data.rows.length, 5);
  // All users matched a dept
  assert(result.data.rows.every(r => r.dept_name !== null));
});

const exitCode = report('query-integration');
process.exit(exitCode);
