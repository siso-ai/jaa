import { test, assert, assertEqual, assertThrows, report } from './runner.js';
import { MemoryStore } from '../src/persistence/Store.js';

let store;

function fresh() {
  store = new MemoryStore();
}

// ── put ─────────────────────────────────────────

test('Store: put returns a hash string', () => {
  fresh();
  const hash = store.put({ name: 'Alice' });
  assertEqual(typeof hash, 'string');
  assertEqual(hash.length, 64, 'SHA-256 hex is 64 chars');
});

test('Store: same content returns same hash', () => {
  fresh();
  const h1 = store.put({ a: 1 });
  const h2 = store.put({ a: 1 });
  assertEqual(h1, h2);
});

test('Store: different content returns different hash', () => {
  fresh();
  const h1 = store.put({ a: 1 });
  const h2 = store.put({ a: 2 });
  assert(h1 !== h2, 'hashes should differ');
});

test('Store: key order does not affect hash', () => {
  fresh();
  const h1 = store.put({ a: 1, b: 2 });
  const h2 = store.put({ b: 2, a: 1 });
  assertEqual(h1, h2);
});

// ── get ─────────────────────────────────────────

test('Store: get returns stored content', () => {
  fresh();
  const hash = store.put({ name: 'Alice', age: 30 });
  const result = store.get(hash);
  assertEqual(result.name, 'Alice');
  assertEqual(result.age, 30);
});

test('Store: get returns a clone, not a reference', () => {
  fresh();
  const hash = store.put({ x: 1 });
  const result = store.get(hash);
  result.x = 999;
  assertEqual(store.get(hash).x, 1);
});

test('Store: get throws on missing hash', () => {
  fresh();
  assertThrows(
    () => store.get('0000000000000000000000000000000000000000000000000000000000000000'),
    'Object not found'
  );
});

// ── has ─────────────────────────────────────────

test('Store: has returns true for stored content', () => {
  fresh();
  const hash = store.put({ a: 1 });
  assert(store.has(hash));
});

test('Store: has returns false for missing hash', () => {
  fresh();
  assert(!store.has('nonexistent'));
});

// ── value types ─────────────────────────────────

test('Store: stores strings', () => {
  fresh();
  const hash = store.put('hello world');
  assertEqual(store.get(hash), 'hello world');
});

test('Store: stores numbers', () => {
  fresh();
  const hash = store.put(42);
  assertEqual(store.get(hash), 42);
});

test('Store: stores arrays', () => {
  fresh();
  const hash = store.put([1, 2, 3]);
  const result = store.get(hash);
  assertEqual(result[0], 1);
  assertEqual(result[1], 2);
  assertEqual(result[2], 3);
  assertEqual(result.length, 3);
});

test('Store: stores null', () => {
  fresh();
  const hash = store.put(null);
  assertEqual(store.get(hash), null);
});

test('Store: stores booleans', () => {
  fresh();
  const h1 = store.put(true);
  const h2 = store.put(false);
  assertEqual(store.get(h1), true);
  assertEqual(store.get(h2), false);
  assert(h1 !== h2);
});

test('Store: stores nested structures', () => {
  fresh();
  const data = { users: [{ name: 'Alice' }, { name: 'Bob' }], count: 2 };
  const hash = store.put(data);
  const result = store.get(hash);
  assertEqual(result.users[1].name, 'Bob');
  assertEqual(result.count, 2);
});

test('Store: stores empty structures', () => {
  fresh();
  const h1 = store.put({});
  const h2 = store.put([]);
  assertEqual(Object.keys(store.get(h1)).length, 0);
  assertEqual(store.get(h2).length, 0);
});

// ── deduplication ───────────────────────────────

test('Store: deduplication — internal map does not grow', () => {
  fresh();
  store.put({ x: 1 });
  store.put({ x: 1 });
  store.put({ x: 1 });
  assertEqual(store.objects.size, 1);
});

const exitCode = report('store');
process.exit(exitCode);
