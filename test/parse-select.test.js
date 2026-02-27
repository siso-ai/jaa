import { test, assert, assertEqual, report } from './runner.js';
import { tokenize } from '../src/gates/query/sql/tokenizer.js';
import { SelectParseGate } from '../src/gates/query/sql/SelectParseGate.js';
import { Event } from '../src/core/Event.js';

const gate = new SelectParseGate();
function parse(sql) {
  const tokens = tokenize(sql);
  return gate.transform(new Event('select_parse', { sql, tokens }));
}

function findStep(pipeline, type) {
  return pipeline.find(s => s.type === type);
}

// ── basic SELECT ────────────────────────────────

test('Parse SELECT: * FROM table', () => {
  const r = parse('SELECT * FROM users');
  assertEqual(r.type, 'query_plan');
  const scan = findStep(r.data.pipeline, 'table_scan');
  assert(scan !== undefined);
  assertEqual(scan.data.table, 'users');
});

test('Parse SELECT: specific columns', () => {
  const r = parse('SELECT name, age FROM users');
  const project = findStep(r.data.pipeline, 'project');
  assert(project !== undefined);
  assertEqual(project.data.columns.length, 2);
  assertEqual(project.data.columns[0], 'name');
  assertEqual(project.data.columns[1], 'age');
});

// ── WHERE ───────────────────────────────────────

test('Parse SELECT: WHERE clause', () => {
  const r = parse('SELECT * FROM users WHERE age > 21');
  const filter = findStep(r.data.pipeline, 'filter');
  assert(filter !== undefined);
  assertEqual(filter.data.where.column, 'age');
  assertEqual(filter.data.where.op, '>');
  assertEqual(filter.data.where.value, 21);
});

test('Parse SELECT: WHERE with AND', () => {
  const r = parse("SELECT * FROM users WHERE age > 21 AND name = 'Alice'");
  const filter = findStep(r.data.pipeline, 'filter');
  assert(filter.data.where.and !== undefined);
  assertEqual(filter.data.where.and.length, 2);
});

test('Parse SELECT: WHERE with OR', () => {
  const r = parse("SELECT * FROM users WHERE age > 30 OR dept = 'eng'");
  const filter = findStep(r.data.pipeline, 'filter');
  assert(filter.data.where.or !== undefined);
});

test('Parse SELECT: WHERE with IN', () => {
  const r = parse("SELECT * FROM users WHERE dept IN ('eng', 'sales')");
  const filter = findStep(r.data.pipeline, 'filter');
  assertEqual(filter.data.where.op, 'in');
  assertEqual(filter.data.where.value.length, 2);
});

test('Parse SELECT: WHERE with LIKE', () => {
  const r = parse("SELECT * FROM users WHERE name LIKE 'A%'");
  const filter = findStep(r.data.pipeline, 'filter');
  assertEqual(filter.data.where.op, 'like');
  assertEqual(filter.data.where.value, 'A%');
});

test('Parse SELECT: WHERE IS NULL', () => {
  const r = parse('SELECT * FROM users WHERE email IS NULL');
  const filter = findStep(r.data.pipeline, 'filter');
  assertEqual(filter.data.where.op, 'is_null');
});

// ── ORDER BY ────────────────────────────────────

test('Parse SELECT: ORDER BY', () => {
  const r = parse('SELECT * FROM users ORDER BY name ASC');
  const order = findStep(r.data.pipeline, 'order_by');
  assert(order !== undefined);
  assertEqual(order.data.order[0].column, 'name');
  assertEqual(order.data.order[0].direction, 'asc');
});

test('Parse SELECT: ORDER BY DESC', () => {
  const r = parse('SELECT * FROM users ORDER BY age DESC');
  const order = findStep(r.data.pipeline, 'order_by');
  assertEqual(order.data.order[0].direction, 'desc');
});

test('Parse SELECT: ORDER BY multiple columns', () => {
  const r = parse('SELECT * FROM users ORDER BY dept ASC, age DESC');
  const order = findStep(r.data.pipeline, 'order_by');
  assertEqual(order.data.order.length, 2);
  assertEqual(order.data.order[0].column, 'dept');
  assertEqual(order.data.order[1].column, 'age');
  assertEqual(order.data.order[1].direction, 'desc');
});

// ── LIMIT / OFFSET ──────────────────────────────

test('Parse SELECT: LIMIT', () => {
  const r = parse('SELECT * FROM users LIMIT 10');
  const limit = findStep(r.data.pipeline, 'limit');
  assert(limit !== undefined);
  assertEqual(limit.data.limit, 10);
});

test('Parse SELECT: LIMIT with OFFSET', () => {
  const r = parse('SELECT * FROM users LIMIT 10 OFFSET 20');
  const limit = findStep(r.data.pipeline, 'limit');
  assertEqual(limit.data.limit, 10);
  assertEqual(limit.data.offset, 20);
});

// ── DISTINCT ────────────────────────────────────

test('Parse SELECT: DISTINCT', () => {
  const r = parse('SELECT DISTINCT dept FROM users');
  const dist = findStep(r.data.pipeline, 'distinct');
  assert(dist !== undefined);
});

// ── GROUP BY ────────────────────────────────────

test('Parse SELECT: GROUP BY with COUNT', () => {
  const r = parse('SELECT dept, COUNT(*) AS cnt FROM users GROUP BY dept');
  const agg = findStep(r.data.pipeline, 'aggregate');
  assert(agg !== undefined);
  assertEqual(agg.data.groupBy[0], 'dept');
  assert(agg.data.aggregates.some(a => a.fn === 'COUNT'));
});

// ── JOIN ────────────────────────────────────────

test('Parse SELECT: JOIN', () => {
  const r = parse('SELECT * FROM users JOIN orders ON user_id = id');
  const join = findStep(r.data.pipeline, 'join');
  assert(join !== undefined);
  assertEqual(join.data.right.table, 'orders');
  assertEqual(join.data.type, 'inner');
});

test('Parse SELECT: LEFT JOIN', () => {
  const r = parse('SELECT * FROM users LEFT JOIN orders ON user_id = id');
  const join = findStep(r.data.pipeline, 'join');
  assertEqual(join.data.type, 'left');
});

// ── pipeline ordering ───────────────────────────

test('Parse SELECT: pipeline order is scan → filter → project → order → limit', () => {
  const r = parse('SELECT name, age FROM users WHERE age > 21 ORDER BY name LIMIT 10');
  const types = r.data.pipeline.map(s => s.type);
  const scanIdx = types.indexOf('table_scan');
  const filterIdx = types.indexOf('filter');
  const projectIdx = types.indexOf('project');
  const orderIdx = types.indexOf('order_by');
  const limitIdx = types.indexOf('limit');

  assert(scanIdx < filterIdx, 'scan before filter');
  assert(filterIdx < projectIdx, 'filter before project');
  assert(projectIdx < orderIdx, 'project before order');
  assert(orderIdx < limitIdx, 'order before limit');
});

// ── complex query ───────────────────────────────

test('Parse SELECT: complex multi-clause', () => {
  const r = parse("SELECT name, age FROM users WHERE age > 21 AND dept = 'eng' ORDER BY age DESC LIMIT 5 OFFSET 10");
  assertEqual(r.type, 'query_plan');
  assert(findStep(r.data.pipeline, 'table_scan') !== undefined);
  assert(findStep(r.data.pipeline, 'filter') !== undefined);
  assert(findStep(r.data.pipeline, 'project') !== undefined);
  assert(findStep(r.data.pipeline, 'order_by') !== undefined);
  assert(findStep(r.data.pipeline, 'limit') !== undefined);
  assertEqual(findStep(r.data.pipeline, 'limit').data.offset, 10);
});

const exitCode = report('parse-select');
process.exit(exitCode);
