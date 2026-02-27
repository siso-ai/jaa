import { test, assert, assertEqual, report } from './runner.js';
import { Runner } from '../src/resolution/Runner.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { MemoryRefs } from '../src/persistence/Refs.js';
import { PureGate } from '../src/protocol/PureGate.js';
import { StateGate } from '../src/protocol/StateGate.js';
import { ReadSet } from '../src/protocol/ReadSet.js';
import { MutationBatch } from '../src/protocol/MutationBatch.js';
import { Event } from '../src/core/Event.js';

let store, refs;

function fresh() {
  store = new MemoryStore();
  refs = new MemoryRefs();
}

// ── StateGate transform error ───────────────────

test('Runner+Error: StateGate transform error emits error event', () => {
  fresh();
  class BadGate extends StateGate {
    constructor() { super('bad'); }
    reads(event) { return new ReadSet(); }
    transform(event, state) { throw new Error('oops'); }
  }
  const runner = new Runner({ store, refs });
  runner.register(new BadGate());
  runner.emit(new Event('bad', {}));
  const sample = runner.sample();
  assert(sample.pending.some(e => e.type === 'error'), 'should have error event');
  const err = sample.pending.find(e => e.type === 'error');
  assert(err.data.message.includes('oops'));
  assertEqual(err.data.source, 'bad');
});

test('Runner+Error: StateGate error does not apply mutations', () => {
  fresh();
  class WriteAndDie extends StateGate {
    constructor() { super('write_die'); }
    reads(event) { return new ReadSet(); }
    transform(event, state) {
      // Build a batch but then throw before returning
      const batch = new MutationBatch()
        .put('data', { should: 'not exist' })
        .refSet('should_not_exist', 0);
      throw new Error('died before returning');
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new WriteAndDie());
  runner.emit(new Event('write_die', {}));
  // Mutations were never returned, so never applied
  assertEqual(refs.get('should_not_exist'), null);
});

// ── PureGate transform error ────────────────────

test('Runner+Error: PureGate transform error emits error event', () => {
  fresh();
  class BadPure extends PureGate {
    constructor() { super('bad_pure'); }
    transform(event) { throw new Error('pure oops'); }
  }
  const runner = new Runner({ store, refs });
  runner.register(new BadPure());
  runner.emit(new Event('bad_pure', {}));
  const sample = runner.sample();
  const err = sample.pending.find(e => e.type === 'error');
  assert(err !== undefined, 'should have error event');
  assert(err.data.message.includes('pure oops'));
  assertEqual(err.data.source, 'bad_pure');
});

// ── missing ref is not an error ─────────────────

test('Runner+Error: missing ref in reads does not throw', () => {
  fresh();
  class ReadMissing extends StateGate {
    constructor() { super('read_missing'); }
    reads(event) { return new ReadSet().ref('nope'); }
    transform(event, state) {
      return new MutationBatch()
        .emit(new Event('result', { found: state.refs['nope'] !== null }));
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new ReadMissing());
  runner.emit(new Event('read_missing', {}));
  const result = runner.sample().pending[0];
  assertEqual(result.type, 'result');
  assertEqual(result.data.found, false);
});

// ── corrupt ref (points to missing hash) ────────

test('Runner+Error: ref pointing to missing hash emits error', () => {
  fresh();
  // Set a ref to a hash that doesn't exist in the store
  refs.set('corrupt', 'deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef');

  class ReadCorrupt extends StateGate {
    constructor() { super('read_corrupt'); }
    reads(event) { return new ReadSet().ref('corrupt'); }
    transform(event, state) {
      return new MutationBatch().emit(new Event('never_reached', {}));
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new ReadCorrupt());
  runner.emit(new Event('read_corrupt', {}));
  const sample = runner.sample();
  const err = sample.pending.find(e => e.type === 'error');
  assert(err !== undefined, 'should have error for corrupt ref');
  assert(err.data.message.includes('not found'), 'should mention missing object');
});

// ── error doesn't stop processing ───────────────

test('Runner+Error: error in one gate does not stop later emits', () => {
  fresh();
  class BadGate extends PureGate {
    constructor() { super('bad'); }
    transform(event) { throw new Error('fail'); }
  }
  const runner = new Runner({ store, refs });
  runner.register(new BadGate());
  runner.emit(new Event('bad', {}));
  runner.emit(new Event('unclaimed', { val: 42 }));
  const sample = runner.sample();
  assert(sample.pending.some(e => e.type === 'error'));
  assert(sample.pending.some(e => e.type === 'unclaimed'));
});

const exitCode = report('runner-error');
process.exit(exitCode);
