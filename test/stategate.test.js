import { test, assert, assertEqual, report } from './runner.js';
import { StateGate } from '../src/protocol/StateGate.js';
import { ReadSet } from '../src/protocol/ReadSet.js';
import { MutationBatch } from '../src/protocol/MutationBatch.js';
import { Gate } from '../src/core/Gate.js';
import { Event } from '../src/core/Event.js';

// ── signature ───────────────────────────────────

test('StateGate: has a signature', () => {
  class TestGate extends StateGate {
    constructor() { super('test'); }
    reads(event) { return new ReadSet(); }
    transform(event, state) { return new MutationBatch(); }
  }
  assertEqual(new TestGate().signature, 'test');
});

// ── reads ───────────────────────────────────────

test('StateGate: reads returns a ReadSet', () => {
  class InsertGate extends StateGate {
    constructor() { super('insert'); }
    reads(event) {
      return new ReadSet()
        .ref(`db/tables/${event.data.table}/schema`)
        .ref(`db/tables/${event.data.table}/next_id`);
    }
    transform(event, state) { return new MutationBatch(); }
  }
  const gate = new InsertGate();
  const rs = gate.reads(new Event('insert', { table: 'users' }));
  assertEqual(rs.refs.length, 2);
  assertEqual(rs.refs[0], 'db/tables/users/schema');
  assertEqual(rs.refs[1], 'db/tables/users/next_id');
});

test('StateGate: reads with patterns', () => {
  class ScanGate extends StateGate {
    constructor() { super('scan'); }
    reads(event) {
      return new ReadSet()
        .pattern(`db/tables/${event.data.table}/rows/`);
    }
    transform(event, state) { return new MutationBatch(); }
  }
  const gate = new ScanGate();
  const rs = gate.reads(new Event('scan', { table: 'orders' }));
  assertEqual(rs.patterns.length, 1);
  assertEqual(rs.patterns[0], 'db/tables/orders/rows/');
});

test('StateGate: reads is pure (same input → same output)', () => {
  class ReadGate extends StateGate {
    constructor() { super('read'); }
    reads(event) { return new ReadSet().ref(`key/${event.data.id}`); }
    transform(event, state) { return new MutationBatch(); }
  }
  const gate = new ReadGate();
  const r1 = gate.reads(new Event('read', { id: 'x' }));
  const r2 = gate.reads(new Event('read', { id: 'x' }));
  assertEqual(r1.refs[0], r2.refs[0]);
});

// ── transform ───────────────────────────────────

test('StateGate: transform receives state and returns MutationBatch', () => {
  class CounterGate extends StateGate {
    constructor() { super('increment'); }
    reads(event) { return new ReadSet().ref('counter'); }
    transform(event, state) {
      const current = state.refs['counter'] || 0;
      return new MutationBatch()
        .put('counter', current + 1)
        .refSet('counter', 0);
    }
  }
  const gate = new CounterGate();
  const result = gate.transform(
    new Event('increment', {}),
    { refs: { 'counter': 5 }, patterns: {} }
  );
  assert(result instanceof MutationBatch);
  assertEqual(result.puts[0].content, 6);
  assertEqual(result.refSets[0].name, 'counter');
  assertEqual(result.refSets[0].putIndex, 0);
});

test('StateGate: transform can return events-only batch', () => {
  class QueryGate extends StateGate {
    constructor() { super('query'); }
    reads(event) {
      return new ReadSet().pattern(`db/tables/${event.data.table}/rows/`);
    }
    transform(event, state) {
      const rows = Object.values(
        state.patterns[`db/tables/${event.data.table}/rows/`] || {}
      );
      return new MutationBatch()
        .emit(new Event('query_result', { rows }));
    }
  }
  const gate = new QueryGate();
  const result = gate.transform(
    new Event('query', { table: 'users' }),
    {
      refs: {},
      patterns: {
        'db/tables/users/rows/': {
          'db/tables/users/rows/1': { id: 1, name: 'Alice' },
          'db/tables/users/rows/2': { id: 2, name: 'Bob' }
        }
      }
    }
  );
  assertEqual(result.puts.length, 0);
  assertEqual(result.events.length, 1);
  assertEqual(result.events[0].data.rows.length, 2);
});

test('StateGate: transform with null state values', () => {
  class CheckGate extends StateGate {
    constructor() { super('check'); }
    reads(event) { return new ReadSet().ref('missing'); }
    transform(event, state) {
      const exists = state.refs['missing'] !== null;
      return new MutationBatch()
        .emit(new Event('check_result', { exists }));
    }
  }
  const gate = new CheckGate();
  const result = gate.transform(
    new Event('check', {}),
    { refs: { 'missing': null }, patterns: {} }
  );
  assertEqual(result.events[0].data.exists, false);
});

// ── inheritance ─────────────────────────────────

test('StateGate: extends Gate', () => {
  const gate = new StateGate('test');
  assert(gate instanceof Gate);
});

test('StateGate: base reads returns empty ReadSet', () => {
  const gate = new StateGate('base');
  const rs = gate.reads(new Event('base', {}));
  assertEqual(rs.refs.length, 0);
  assertEqual(rs.patterns.length, 0);
});

test('StateGate: base transform returns empty MutationBatch', () => {
  const gate = new StateGate('base');
  const mb = gate.transform(new Event('base', {}), { refs: {}, patterns: {} });
  assertEqual(mb.puts.length, 0);
  assertEqual(mb.refSets.length, 0);
  assertEqual(mb.refDeletes.length, 0);
  assertEqual(mb.events.length, 0);
});

const exitCode = report('stategate');
process.exit(exitCode);
