import { test, assert, assertEqual, report } from './runner.js';
import { MemoryRefs } from '../src/persistence/Refs.js';

let refs;

function fresh() {
  refs = new MemoryRefs();
}

// ── set and get ─────────────────────────────────

test('Refs: set and get', () => {
  fresh();
  refs.set('db/tables/users/schema', 'abc123');
  assertEqual(refs.get('db/tables/users/schema'), 'abc123');
});

test('Refs: get returns null for missing ref', () => {
  fresh();
  assertEqual(refs.get('nonexistent'), null);
});

test('Refs: set overwrites existing ref', () => {
  fresh();
  refs.set('x', 'hash1');
  refs.set('x', 'hash2');
  assertEqual(refs.get('x'), 'hash2');
});

// ── delete ──────────────────────────────────────

test('Refs: delete removes ref', () => {
  fresh();
  refs.set('x', 'hash1');
  refs.delete('x');
  assertEqual(refs.get('x'), null);
});

test('Refs: delete is no-op for missing ref', () => {
  fresh();
  refs.delete('nonexistent'); // should not throw
  assertEqual(refs.get('nonexistent'), null);
});

// ── list ────────────────────────────────────────

test('Refs: list returns matching names', () => {
  fresh();
  refs.set('db/tables/users/rows/1', 'h1');
  refs.set('db/tables/users/rows/2', 'h2');
  refs.set('db/tables/users/rows/3', 'h3');
  refs.set('db/tables/orders/rows/1', 'h4');
  const result = refs.list('db/tables/users/rows/');
  assertEqual(result.length, 3);
  assert(result.includes('db/tables/users/rows/1'));
  assert(result.includes('db/tables/users/rows/2'));
  assert(result.includes('db/tables/users/rows/3'));
});

test('Refs: list returns sorted results', () => {
  fresh();
  refs.set('db/c', 'h1');
  refs.set('db/a', 'h2');
  refs.set('db/b', 'h3');
  const result = refs.list('db/');
  assertEqual(result[0], 'db/a');
  assertEqual(result[1], 'db/b');
  assertEqual(result[2], 'db/c');
});

test('Refs: list returns empty array for no matches', () => {
  fresh();
  refs.set('other/key', 'h1');
  const result = refs.list('db/');
  assertEqual(result.length, 0);
});

test('Refs: list with empty prefix returns all', () => {
  fresh();
  refs.set('a', 'h1');
  refs.set('b', 'h2');
  const result = refs.list('');
  assertEqual(result.length, 2);
});

test('Refs: list does not include partial prefix matches', () => {
  fresh();
  refs.set('db/tables', 'h1');
  refs.set('db/tablespace', 'h2');
  const result = refs.list('db/tables/');
  assertEqual(result.length, 0);
});

const exitCode = report('refs');
process.exit(exitCode);
