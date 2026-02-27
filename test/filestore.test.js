import { test, assert, assertEqual, report } from './runner.js';
import { FileStore } from '../src/persistence/FileStore.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { existsSync, readFileSync, mkdirSync, rmSync } from 'fs';
import { join } from 'path';

const BASE = '/tmp/siso-test-filestore-' + process.pid;

function fresh() {
  if (existsSync(BASE)) rmSync(BASE, { recursive: true });
  mkdirSync(BASE, { recursive: true });
  return new FileStore({ basePath: BASE });
}

// ── put creates file on disk ────────────────────

test('FileStore: put creates file on disk', () => {
  const store = fresh();
  const hash = store.put({ x: 1 });
  const path = join(BASE, 'store', hash.slice(0, 2), hash.slice(2));
  assert(existsSync(path), 'file should exist on disk');
});

// ── file content is canonical JSON ──────────────

test('FileStore: file content is canonical JSON', () => {
  const store = fresh();
  const hash = store.put({ b: 2, a: 1 });
  const path = join(BASE, 'store', hash.slice(0, 2), hash.slice(2));
  const content = readFileSync(path, 'utf8');
  assertEqual(content, '{"a":1,"b":2}');
});

// ── deduplication ───────────────────────────────

test('FileStore: deduplication — same content same hash', () => {
  const store = fresh();
  const h1 = store.put({ x: 1 });
  const h2 = store.put({ x: 1 });
  assertEqual(h1, h2);
});

// ── get reads back correct content ──────────────

test('FileStore: get reads back correct content', () => {
  const store = fresh();
  const hash = store.put({ name: 'Alice', age: 30 });
  const obj = store.get(hash);
  assertEqual(obj.name, 'Alice');
  assertEqual(obj.age, 30);
});

// ── get returns clone ───────────────────────────

test('FileStore: get returns clone (mutating result does not affect store)', () => {
  const store = fresh();
  const hash = store.put({ x: 1 });
  const obj1 = store.get(hash);
  obj1.x = 999;
  const obj2 = store.get(hash);
  assertEqual(obj2.x, 1);
});

// ── get throws on missing hash ──────────────────

test('FileStore: get throws on missing hash', () => {
  const store = fresh();
  let threw = false;
  try {
    store.get('deadbeef'.repeat(8));
  } catch (e) {
    threw = true;
    assert(e.message.includes('not found'));
  }
  assert(threw, 'should throw');
});

// ── has returns true/false ──────────────────────

test('FileStore: has returns true/false correctly', () => {
  const store = fresh();
  const hash = store.put({ x: 1 });
  assertEqual(store.has(hash), true);
  assertEqual(store.has('deadbeef'.repeat(8)), false);
});

// ── all value types ─────────────────────────────

test('FileStore: handles all value types', () => {
  const store = fresh();
  assertEqual(store.get(store.put('hello')), 'hello');
  assertEqual(store.get(store.put(42)), 42);
  assertEqual(store.get(store.put(true)), true);
  assertEqual(store.get(store.put(null)), null);
  assertEqual(store.get(store.put([1, 2, 3]))[1], 2);
  assertEqual(store.get(store.put({ a: { b: 1 } })).a.b, 1);
});

// ── key ordering is canonical ───────────────────

test('FileStore: different key order produces same hash', () => {
  const store = fresh();
  const h1 = store.put({ a: 1, b: 2 });
  const h2 = store.put({ b: 2, a: 1 });
  assertEqual(h1, h2);
});

// ── compatible with MemoryStore ─────────────────

test('FileStore: same content produces same hash as MemoryStore', () => {
  const store = fresh();
  const mem = new MemoryStore();
  const obj = { name: 'test', value: 42 };
  assertEqual(store.put(obj), mem.put(obj));
});

// cleanup
if (existsSync(BASE)) rmSync(BASE, { recursive: true });

const exitCode = report('filestore');
process.exit(exitCode);
