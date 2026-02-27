import { test, assert, assertEqual, report } from './runner.js';
import { canonicalize } from '../src/persistence/canonicalize.js';

// ── Key ordering ────────────────────────────────

test('canonicalize: object keys are sorted', () => {
  assertEqual(
    canonicalize({ b: 2, a: 1 }),
    canonicalize({ a: 1, b: 2 })
  );
});

test('canonicalize: nested object keys are sorted', () => {
  assertEqual(
    canonicalize({ z: { b: 2, a: 1 } }),
    canonicalize({ z: { a: 1, b: 2 } })
  );
});

test('canonicalize: deeply nested keys are sorted', () => {
  const a = { c: { f: { b: 2, a: 1 }, e: 3, d: 4 } };
  const b = { c: { d: 4, e: 3, f: { a: 1, b: 2 } } };
  assertEqual(canonicalize(a), canonicalize(b));
});

// ── Arrays ──────────────────────────────────────

test('canonicalize: arrays preserve order', () => {
  assert(
    canonicalize([3, 1, 2]) !== canonicalize([1, 2, 3]),
    'different order should produce different output'
  );
});

test('canonicalize: arrays with objects sort object keys', () => {
  assertEqual(
    canonicalize([{ b: 2, a: 1 }]),
    canonicalize([{ a: 1, b: 2 }])
  );
});

// ── Determinism ─────────────────────────────────

test('canonicalize: same content always produces same string', () => {
  const input = { x: 1, y: 'hello', z: [1, 2, 3] };
  const first = canonicalize(input);
  for (let i = 0; i < 100; i++) {
    assertEqual(canonicalize(input), first, `iteration ${i}`);
  }
});

test('canonicalize: different content produces different strings', () => {
  assert(
    canonicalize({ a: 1 }) !== canonicalize({ a: 2 }),
    'different values should differ'
  );
});

// ── Primitives ──────────────────────────────────

test('canonicalize: null', () => {
  assertEqual(canonicalize(null), 'null');
});

test('canonicalize: undefined becomes null', () => {
  assertEqual(canonicalize(undefined), 'null');
});

test('canonicalize: numbers', () => {
  assertEqual(canonicalize(42), '42');
  assertEqual(canonicalize(3.14), '3.14');
  assertEqual(canonicalize(0), '0');
  assertEqual(canonicalize(-1), '-1');
});

test('canonicalize: strings', () => {
  assertEqual(canonicalize('hello'), '"hello"');
  assertEqual(canonicalize(''), '""');
});

test('canonicalize: booleans', () => {
  assertEqual(canonicalize(true), 'true');
  assertEqual(canonicalize(false), 'false');
});

// ── Empty structures ────────────────────────────

test('canonicalize: empty object', () => {
  assertEqual(canonicalize({}), '{}');
});

test('canonicalize: empty array', () => {
  assertEqual(canonicalize([]), '[]');
});

// ── Undefined filtering ─────────────────────────

test('canonicalize: undefined values in objects are omitted', () => {
  assertEqual(
    canonicalize({ a: 1, b: undefined }),
    canonicalize({ a: 1 })
  );
});

const exitCode = report('canonicalize');
process.exit(exitCode);
