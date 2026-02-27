import { test, assert, assertEqual, report } from './runner.js';
import { FileRefs } from '../src/persistence/FileRefs.js';
import { existsSync, mkdirSync, rmSync } from 'fs';
import { join } from 'path';

const BASE = '/tmp/siso-test-filerefs-' + process.pid;

function fresh() {
  if (existsSync(BASE)) rmSync(BASE, { recursive: true });
  mkdirSync(BASE, { recursive: true });
  return new FileRefs({ basePath: BASE });
}

// ── set creates file ────────────────────────────

test('FileRefs: set creates file in refs directory', () => {
  const refs = fresh();
  refs.set('key', 'abc123');
  assert(existsSync(join(BASE, 'refs', 'key')), 'file should exist');
});

// ── get reads hash ──────────────────────────────

test('FileRefs: get reads hash from file', () => {
  const refs = fresh();
  refs.set('key', 'abc123');
  assertEqual(refs.get('key'), 'abc123');
});

// ── get returns null for missing ────────────────

test('FileRefs: get returns null for missing ref', () => {
  const refs = fresh();
  assertEqual(refs.get('nope'), null);
});

// ── set overwrites ──────────────────────────────

test('FileRefs: set overwrites existing ref', () => {
  const refs = fresh();
  refs.set('key', 'hash1');
  refs.set('key', 'hash2');
  assertEqual(refs.get('key'), 'hash2');
});

// ── delete removes file ─────────────────────────

test('FileRefs: delete removes file', () => {
  const refs = fresh();
  refs.set('key', 'abc');
  refs.delete('key');
  assertEqual(refs.get('key'), null);
  assert(!existsSync(join(BASE, 'refs', 'key')), 'file should be gone');
});

// ── delete is no-op for missing ─────────────────

test('FileRefs: delete is no-op for missing ref', () => {
  const refs = fresh();
  refs.delete('nope'); // should not throw
});

// ── list returns matching names ─────────────────

test('FileRefs: list returns matching names from directory walk', () => {
  const refs = fresh();
  refs.set('db/tables/users/rows/1', 'h1');
  refs.set('db/tables/users/rows/2', 'h2');
  refs.set('db/tables/users/rows/3', 'h3');
  refs.set('db/tables/users/schema', 'hs');

  const rows = refs.list('db/tables/users/rows/');
  assertEqual(rows.length, 3);
  assert(rows.includes('db/tables/users/rows/1'));
  assert(rows.includes('db/tables/users/rows/2'));
  assert(rows.includes('db/tables/users/rows/3'));
});

// ── list returns sorted ─────────────────────────

test('FileRefs: list returns sorted results', () => {
  const refs = fresh();
  refs.set('c', 'h1');
  refs.set('a', 'h2');
  refs.set('b', 'h3');

  const all = refs.list('');
  assertEqual(all[0], 'a');
  assertEqual(all[1], 'b');
  assertEqual(all[2], 'c');
});

// ── list returns empty for no matches ───────────

test('FileRefs: list returns empty array for no matches', () => {
  const refs = fresh();
  assertEqual(refs.list('nope/').length, 0);
});

// ── nested paths create directory structure ─────

test('FileRefs: nested ref paths create correct directory structure', () => {
  const refs = fresh();
  refs.set('db/tables/users/rows/1', 'hash1');
  assert(existsSync(join(BASE, 'refs', 'db', 'tables', 'users', 'rows', '1')));
});

// ── delete cleans empty directories ─────────────

test('FileRefs: delete cleans up empty parent directories', () => {
  const refs = fresh();
  refs.set('a/b/c/d', 'hash');
  refs.delete('a/b/c/d');
  // d is gone, c/ should be cleaned, b/ should be cleaned, a/ should be cleaned
  assert(!existsSync(join(BASE, 'refs', 'a', 'b', 'c')), 'empty dirs cleaned');
});

// ── compatible with MemoryRefs ──────────────────

test('FileRefs: list matches MemoryRefs behavior', () => {
  const refs = fresh();
  refs.set('db/tables/users/schema', 'h1');
  refs.set('db/tables/users/next_id', 'h2');
  refs.set('db/tables/users/rows/1', 'h3');
  refs.set('db/tables/orders/schema', 'h4');

  const userRefs = refs.list('db/tables/users/rows/');
  assertEqual(userRefs.length, 1);

  const allUsers = refs.list('db/tables/users/');
  assertEqual(allUsers.length, 3); // schema, next_id, rows/1
});

// cleanup
if (existsSync(BASE)) rmSync(BASE, { recursive: true });

const exitCode = report('filerefs');
process.exit(exitCode);
