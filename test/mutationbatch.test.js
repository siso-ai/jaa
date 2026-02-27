import { test, assert, assertEqual, assertThrows, report } from './runner.js';
import { MutationBatch } from '../src/protocol/MutationBatch.js';
import { Event } from '../src/core/Event.js';

// ── put ─────────────────────────────────────────

test('MutationBatch: put adds to puts array', () => {
  const mb = new MutationBatch().put('row', { name: 'Alice' });
  assertEqual(mb.puts.length, 1);
  assertEqual(mb.puts[0].type, 'row');
  assertEqual(mb.puts[0].content.name, 'Alice');
});

test('MutationBatch: multiple puts', () => {
  const mb = new MutationBatch()
    .put('row', { name: 'Alice' })
    .put('counter', '1');
  assertEqual(mb.puts.length, 2);
  assertEqual(mb.puts[1].content, '1');
});

// ── refSet ──────────────────────────────────────

test('MutationBatch: refSet with index references puts array', () => {
  const mb = new MutationBatch()
    .put('row', { name: 'Alice' })
    .refSet('db/tables/users/rows/1', 0);
  assertEqual(mb.refSets.length, 1);
  assertEqual(mb.refSets[0].name, 'db/tables/users/rows/1');
  assertEqual(mb.refSets[0].putIndex, 0);
});

test('MutationBatch: refSet index out of range throws', () => {
  assertThrows(
    () => new MutationBatch().refSet('x', 0),
    'out of range'
  );
});

test('MutationBatch: refSet index at boundary is valid', () => {
  const mb = new MutationBatch()
    .put('row', { x: 1 })
    .put('row', { x: 2 })
    .refSet('a', 0)
    .refSet('b', 1);
  assertEqual(mb.refSets.length, 2);
  assertEqual(mb.refSets[0].putIndex, 0);
  assertEqual(mb.refSets[1].putIndex, 1);
});

// ── refSetHash ──────────────────────────────────

test('MutationBatch: refSetHash stores literal hash', () => {
  const mb = new MutationBatch()
    .refSetHash('db/x', 'abc123');
  assertEqual(mb.refSets.length, 1);
  assertEqual(mb.refSets[0].name, 'db/x');
  assertEqual(mb.refSets[0].hash, 'abc123');
  assertEqual(mb.refSets[0].putIndex, undefined);
});

// ── refDelete ───────────────────────────────────

test('MutationBatch: refDelete adds to refDeletes', () => {
  const mb = new MutationBatch()
    .refDelete('db/tables/users/rows/1');
  assertEqual(mb.refDeletes.length, 1);
  assertEqual(mb.refDeletes[0], 'db/tables/users/rows/1');
});

test('MutationBatch: multiple refDeletes', () => {
  const mb = new MutationBatch()
    .refDelete('db/tables/users/rows/1')
    .refDelete('db/tables/users/rows/2');
  assertEqual(mb.refDeletes.length, 2);
});

// ── emit ────────────────────────────────────────

test('MutationBatch: emit adds event to events list', () => {
  const mb = new MutationBatch()
    .emit(new Event('result', { rows: [] }));
  assertEqual(mb.events.length, 1);
  assertEqual(mb.events[0].type, 'result');
});

test('MutationBatch: multiple emits', () => {
  const mb = new MutationBatch()
    .emit(new Event('a', {}))
    .emit(new Event('b', {}));
  assertEqual(mb.events.length, 2);
  assertEqual(mb.events[0].type, 'a');
  assertEqual(mb.events[1].type, 'b');
});

// ── full chain ──────────────────────────────────

test('MutationBatch: full chain', () => {
  const mb = new MutationBatch()
    .put('row', { name: 'Alice' })
    .refSet('db/tables/users/rows/1', 0)
    .put('counter', '2')
    .refSet('db/tables/users/next_id', 1)
    .emit(new Event('insert_result', { id: 1 }));
  assertEqual(mb.puts.length, 2);
  assertEqual(mb.refSets.length, 2);
  assertEqual(mb.refDeletes.length, 0);
  assertEqual(mb.events.length, 1);
});

// ── empty ───────────────────────────────────────

test('MutationBatch: empty batch', () => {
  const mb = new MutationBatch();
  assertEqual(mb.puts.length, 0);
  assertEqual(mb.refSets.length, 0);
  assertEqual(mb.refDeletes.length, 0);
  assertEqual(mb.events.length, 0);
});

// ── events-only ─────────────────────────────────

test('MutationBatch: events-only batch (for queries)', () => {
  const mb = new MutationBatch()
    .emit(new Event('result_set', { rows: [{ id: 1 }] }));
  assertEqual(mb.puts.length, 0);
  assertEqual(mb.refSets.length, 0);
  assertEqual(mb.refDeletes.length, 0);
  assertEqual(mb.events.length, 1);
  assertEqual(mb.events[0].data.rows[0].id, 1);
});

// ── mixed refSet and refSetHash ─────────────────

test('MutationBatch: mixed refSet and refSetHash', () => {
  const mb = new MutationBatch()
    .put('schema', { name: 'users' })
    .refSet('db/tables/users/schema', 0)
    .refSetHash('db/tables/users/old_schema', 'deadbeef');
  assertEqual(mb.refSets.length, 2);
  assertEqual(mb.refSets[0].putIndex, 0);
  assertEqual(mb.refSets[0].hash, undefined);
  assertEqual(mb.refSets[1].putIndex, undefined);
  assertEqual(mb.refSets[1].hash, 'deadbeef');
});

const exitCode = report('mutationbatch');
process.exit(exitCode);
