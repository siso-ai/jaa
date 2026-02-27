import { test, assert, assertEqual, report } from './runner.js';
import { Recovery } from '../src/persistence/Recovery.js';
import { FileStore } from '../src/persistence/FileStore.js';
import { FileRefs } from '../src/persistence/FileRefs.js';
import { canonicalize } from '../src/persistence/canonicalize.js';
import { createHash } from 'crypto';
import { existsSync, mkdirSync, rmSync, writeFileSync } from 'fs';
import { join } from 'path';

const BASE = '/tmp/siso-test-recovery-' + process.pid;

function fresh() {
  if (existsSync(BASE)) rmSync(BASE, { recursive: true });
  mkdirSync(BASE, { recursive: true });
  return {
    store: new FileStore({ basePath: BASE }),
    refs: new FileRefs({ basePath: BASE }),
    recovery: new Recovery(BASE),
  };
}

function hashOf(content) {
  return createHash('sha256').update(canonicalize(content)).digest('hex');
}

// ── clean state ─────────────────────────────────

test('Recovery: clean state — no pending.json', () => {
  const { recovery } = fresh();
  const status = recovery.check();
  assertEqual(status.clean, true);
  assertEqual(status.pending, null);
});

// ── pending batch found ─────────────────────────

test('Recovery: pending batch found', () => {
  const { recovery } = fresh();
  // Write a pending.json manually
  mkdirSync(join(BASE, 'wal'), { recursive: true });
  writeFileSync(join(BASE, 'wal', 'pending.json'), JSON.stringify({
    timestamp: Date.now(),
    puts: [{ hash: 'abc', content: '"hello"', applied: false }],
    refSets: [{ name: 'key', hash: 'abc', applied: false }],
    refDeletes: [],
  }));

  const status = recovery.check();
  assertEqual(status.clean, false);
  assert(status.pending !== null);
  assertEqual(status.pending.puts.length, 1);
});

// ── recover replays unapplied ref sets ──────────

test('Recovery: recover replays unapplied operations', () => {
  const { store, refs, recovery } = fresh();

  // Pre-store the content so hash exists
  const content = { name: 'recovered' };
  const canonical = canonicalize(content);
  const hash = hashOf(content);
  store.put(content);

  // Write pending.json with unapplied refSet
  mkdirSync(join(BASE, 'wal'), { recursive: true });
  writeFileSync(join(BASE, 'wal', 'pending.json'), JSON.stringify({
    timestamp: Date.now(),
    puts: [{ hash, content: canonical, applied: true }],
    refSets: [{ name: 'db/test', hash, applied: false }],
    refDeletes: [],
  }));

  // Ref should not exist yet
  assertEqual(refs.get('db/test'), null);

  // Recover
  recovery.recover(store, refs);

  // Ref should now exist
  assertEqual(refs.get('db/test'), hash);

  // pending.json should be deleted
  assert(!existsSync(join(BASE, 'wal', 'pending.json')));
});

// ── recover is idempotent ───────────────────────

test('Recovery: recover is idempotent', () => {
  const { store, refs, recovery } = fresh();

  const content = { val: 42 };
  const hash = hashOf(content);
  store.put(content);

  mkdirSync(join(BASE, 'wal'), { recursive: true });
  writeFileSync(join(BASE, 'wal', 'pending.json'), JSON.stringify({
    timestamp: Date.now(),
    puts: [],
    refSets: [{ name: 'key', hash, applied: false }],
    refDeletes: [],
  }));

  recovery.recover(store, refs);
  assertEqual(refs.get('key'), hash);

  // Second recover is a no-op (pending.json already gone)
  recovery.recover(store, refs);
  assertEqual(refs.get('key'), hash);
});

// ── recover handles mixed applied/unapplied ────

test('Recovery: handles mixed applied/unapplied operations', () => {
  const { store, refs, recovery } = fresh();

  const c1 = { a: 1 };
  const c2 = { b: 2 };
  const h1 = hashOf(c1);
  const h2 = hashOf(c2);
  store.put(c1);
  // c2 not stored — should be replayed from WAL

  // Simulate: first refSet was applied, second was not
  refs.set('applied', h1);

  mkdirSync(join(BASE, 'wal'), { recursive: true });
  writeFileSync(join(BASE, 'wal', 'pending.json'), JSON.stringify({
    timestamp: Date.now(),
    puts: [
      { hash: h1, content: canonicalize(c1), applied: true },
      { hash: h2, content: canonicalize(c2), applied: false },
    ],
    refSets: [
      { name: 'applied', hash: h1, applied: true },
      { name: 'pending', hash: h2, applied: false },
    ],
    refDeletes: [{ name: 'to_delete', applied: false }],
  }));

  // Pre-set a ref to be deleted
  refs.set('to_delete', 'whatever');

  recovery.recover(store, refs);

  // Both refs set
  assertEqual(refs.get('applied'), h1);
  assertEqual(refs.get('pending'), h2);

  // c2 should now be in store (replayed from WAL)
  assert(store.has(h2));

  // Deleted ref gone
  assertEqual(refs.get('to_delete'), null);
});

// ── begin + commit lifecycle ────────────────────

test('Recovery: begin writes WAL, commit clears it', () => {
  const { recovery } = fresh();

  const batch = recovery.begin(
    [{ hash: 'h1', content: '"test"' }],
    [{ name: 'key', hash: 'h1' }],
    ['old_key']
  );

  assert(existsSync(join(BASE, 'wal', 'pending.json')));
  assertEqual(batch.puts.length, 1);
  assertEqual(batch.puts[0].applied, false);

  recovery.commit();
  assert(!existsSync(join(BASE, 'wal', 'pending.json')));
});

// cleanup
if (existsSync(BASE)) rmSync(BASE, { recursive: true });

const exitCode = report('recovery');
process.exit(exitCode);
