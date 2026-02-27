import { test, assert, assertEqual, report } from './runner.js';
import { evaluateCondition, evaluateExpression } from '../src/gates/database/expression.js';

const row = { id: 1, name: 'Alice', age: 30, email: 'alice@example.com', dept: 'eng', salary: null };

// ── evaluateCondition: comparisons ──────────────

test('Expr: equality', () => {
  assert(evaluateCondition({ column: 'name', op: '=', value: 'Alice' }, row));
  assert(!evaluateCondition({ column: 'name', op: '=', value: 'Bob' }, row));
});

test('Expr: inequality', () => {
  assert(evaluateCondition({ column: 'name', op: '!=', value: 'Bob' }, row));
  assert(evaluateCondition({ column: 'name', op: '<>', value: 'Bob' }, row));
});

test('Expr: greater/less than', () => {
  assert(evaluateCondition({ column: 'age', op: '>', value: 25 }, row));
  assert(!evaluateCondition({ column: 'age', op: '>', value: 35 }, row));
  assert(evaluateCondition({ column: 'age', op: '<', value: 35 }, row));
  assert(evaluateCondition({ column: 'age', op: '>=', value: 30 }, row));
  assert(evaluateCondition({ column: 'age', op: '<=', value: 30 }, row));
});

test('Expr: IN list', () => {
  assert(evaluateCondition({ column: 'dept', op: 'in', value: ['eng', 'sales'] }, row));
  assert(!evaluateCondition({ column: 'dept', op: 'in', value: ['hr', 'sales'] }, row));
});

test('Expr: LIKE pattern', () => {
  assert(evaluateCondition({ column: 'email', op: 'like', value: '%@example.com' }, row));
  assert(evaluateCondition({ column: 'name', op: 'like', value: 'Ali%' }, row));
  assert(evaluateCondition({ column: 'name', op: 'like', value: 'A_ice' }, row));
  assert(!evaluateCondition({ column: 'name', op: 'like', value: 'Bob%' }, row));
});

test('Expr: IS NULL / IS NOT NULL', () => {
  assert(evaluateCondition({ column: 'salary', op: 'is_null' }, row));
  assert(!evaluateCondition({ column: 'name', op: 'is_null' }, row));
  assert(evaluateCondition({ column: 'name', op: 'is_not_null' }, row));
  assert(!evaluateCondition({ column: 'salary', op: 'is_not_null' }, row));
});

// ── evaluateCondition: logical ──────────────────

test('Expr: AND', () => {
  assert(evaluateCondition({
    and: [
      { column: 'name', op: '=', value: 'Alice' },
      { column: 'age', op: '>', value: 25 }
    ]
  }, row));
  assert(!evaluateCondition({
    and: [
      { column: 'name', op: '=', value: 'Alice' },
      { column: 'age', op: '>', value: 35 }
    ]
  }, row));
});

test('Expr: OR', () => {
  assert(evaluateCondition({
    or: [
      { column: 'name', op: '=', value: 'Bob' },
      { column: 'age', op: '=', value: 30 }
    ]
  }, row));
  assert(!evaluateCondition({
    or: [
      { column: 'name', op: '=', value: 'Bob' },
      { column: 'age', op: '=', value: 25 }
    ]
  }, row));
});

test('Expr: NOT', () => {
  assert(evaluateCondition({ not: { column: 'name', op: '=', value: 'Bob' } }, row));
  assert(!evaluateCondition({ not: { column: 'name', op: '=', value: 'Alice' } }, row));
});

test('Expr: nested conditions', () => {
  assert(evaluateCondition({
    and: [
      { column: 'dept', op: '=', value: 'eng' },
      { or: [
        { column: 'age', op: '>', value: 40 },
        { column: 'name', op: '=', value: 'Alice' }
      ]}
    ]
  }, row));
});

test('Expr: null/undefined condition returns true', () => {
  assert(evaluateCondition(null, row));
  assert(evaluateCondition(undefined, row));
});

// ── evaluateExpression ──────────────────────────

test('Expr: column reference', () => {
  assertEqual(evaluateExpression('name', row), 'Alice');
  assertEqual(evaluateExpression('age', row), 30);
});

test('Expr: literal values', () => {
  assertEqual(evaluateExpression(42, row), 42);
  assertEqual(evaluateExpression(true, row), true);
  assertEqual(evaluateExpression(null, row), null);
  assertEqual(evaluateExpression({ literal: 'hello' }, row), 'hello');
});

test('Expr: arithmetic', () => {
  assertEqual(evaluateExpression({ op: '+', left: 'age', right: 1 }, row), 31);
  assertEqual(evaluateExpression({ op: '*', left: 'age', right: 2 }, row), 60);
  assertEqual(evaluateExpression({ op: '-', left: 'age', right: 5 }, row), 25);
  assertEqual(evaluateExpression({ op: '/', left: 'age', right: 3 }, row), 10);
  assertEqual(evaluateExpression({ op: '%', left: 'age', right: 7 }, row), 2);
});

test('Expr: division by zero returns null', () => {
  assertEqual(evaluateExpression({ op: '/', left: 'age', right: 0 }, row), null);
});

test('Expr: functions', () => {
  assertEqual(evaluateExpression({ fn: 'UPPER', args: ['name'] }, row), 'ALICE');
  assertEqual(evaluateExpression({ fn: 'LOWER', args: ['name'] }, row), 'alice');
  assertEqual(evaluateExpression({ fn: 'LENGTH', args: ['name'] }, row), 5);
  assertEqual(evaluateExpression({ fn: 'ABS', args: [{ literal: -10 }] }, row), 10);
});

test('Expr: COALESCE', () => {
  assertEqual(evaluateExpression({ coalesce: ['salary', { literal: 0 }] }, row), 0);
  assertEqual(evaluateExpression({ coalesce: ['name', { literal: 'default' }] }, row), 'Alice');
  assertEqual(evaluateExpression({ fn: 'COALESCE', args: ['salary', { literal: 50000 }] }, row), 50000);
});

test('Expr: CASE WHEN', () => {
  const expr = {
    case: [
      { when: { column: 'age', op: '>', value: 40 }, then: { literal: 'senior' } },
      { when: { column: 'age', op: '>', value: 25 }, then: { literal: 'mid' } },
    ],
    else: { literal: 'junior' }
  };
  assertEqual(evaluateExpression(expr, row), 'mid');
  assertEqual(evaluateExpression(expr, { age: 50 }), 'senior');
  assertEqual(evaluateExpression(expr, { age: 20 }), 'junior');
});

const exitCode = report('expression');
process.exit(exitCode);
