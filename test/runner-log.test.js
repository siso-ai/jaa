import { test, assert, assertEqual, report } from './runner.js';
import { Runner } from '../src/resolution/Runner.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { MemoryRefs } from '../src/persistence/Refs.js';
import { PureGate } from '../src/protocol/PureGate.js';
import { StateGate } from '../src/protocol/StateGate.js';
import { ReadSet } from '../src/protocol/ReadSet.js';
import { MutationBatch } from '../src/protocol/MutationBatch.js';
import { StreamLog } from '../src/core/StreamLog.js';
import { Event } from '../src/core/Event.js';

let store, refs;

function fresh() {
  store = new MemoryStore();
  refs = new MemoryRefs();
}

// ── log captures events ─────────────────────────

test('Runner+Log: accepts a log and records events', () => {
  fresh();
  const log = new StreamLog('EVENTS');
  const runner = new Runner({ store, refs, log });
  runner.emit(new Event('unclaimed', { x: 1 }));
  const entries = log.sample().entries;
  assertEqual(entries.length, 1);
  assertEqual(entries[0].type, 'unclaimed');
  assertEqual(entries[0].claimed, null);
});

// ── log captures PureGate claimed events ────────

test('Runner+Log: log records PureGate claimed and follow-up', () => {
  fresh();
  const log = new StreamLog('EVENTS');
  class Echo extends PureGate {
    constructor() { super('echo'); }
    transform(event) {
      return new Event('echoed', event.data);
    }
  }
  const runner = new Runner({ store, refs, log });
  runner.register(new Echo());
  runner.emit(new Event('echo', { val: 1 }));
  const entries = log.sample().entries;
  // First entry: 'echo' claimed by 'echo' gate wrapper
  assertEqual(entries[0].type, 'echo');
  assertEqual(entries[0].claimed, 'echo');
  // Second entry: 'echoed' unclaimed (goes to pending)
  assertEqual(entries[1].type, 'echoed');
  assertEqual(entries[1].claimed, null);
});

// ── log captures StateGate with follow-up events ─

test('Runner+Log: log records StateGate and follow-up events', () => {
  fresh();
  const log = new StreamLog('EVENTS');

  class CreateThing extends StateGate {
    constructor() { super('create'); }
    reads(event) { return new ReadSet(); }
    transform(event, state) {
      return new MutationBatch()
        .put('thing', { name: event.data.name })
        .refSet(`things/${event.data.name}`, 0)
        .emit(new Event('created', { name: event.data.name }));
    }
  }

  const runner = new Runner({ store, refs, log });
  runner.register(new CreateThing());
  runner.emit(new Event('create', { name: 'widget' }));

  const entries = log.sample().entries;
  // Entry 0: 'create' claimed by 'create'
  assertEqual(entries[0].type, 'create');
  assertEqual(entries[0].claimed, 'create');
  // Entry 1: 'created' follow-up, unclaimed
  assertEqual(entries[1].type, 'created');
  assertEqual(entries[1].claimed, null);
});

// ── log captures mixed chain ────────────────────

test('Runner+Log: full chain Pure → State → pending logged', () => {
  fresh();
  const log = new StreamLog('EVENTS');

  class Parse extends PureGate {
    constructor() { super('parse'); }
    transform(event) {
      return new Event('execute', { val: event.data.val });
    }
  }

  class Execute extends StateGate {
    constructor() { super('execute'); }
    reads(event) { return new ReadSet(); }
    transform(event, state) {
      return new MutationBatch()
        .put('data', event.data.val)
        .refSet('result', 0)
        .emit(new Event('done', {}));
    }
  }

  const runner = new Runner({ store, refs, log });
  runner.register(new Parse());
  runner.register(new Execute());
  runner.emit(new Event('parse', { val: 42 }));

  const entries = log.sample().entries;
  assertEqual(entries.length, 3);
  // parse → claimed
  assertEqual(entries[0].type, 'parse');
  assertEqual(entries[0].claimed, 'parse');
  // execute → claimed (emitted by Parse wrapper)
  assertEqual(entries[1].type, 'execute');
  assertEqual(entries[1].claimed, 'execute');
  // done → unclaimed (emitted by Execute batch)
  assertEqual(entries[2].type, 'done');
  assertEqual(entries[2].claimed, null);
});

// ── log captures error events ───────────────────

test('Runner+Log: error events are logged', () => {
  fresh();
  const log = new StreamLog('EVENTS');

  class BadGate extends PureGate {
    constructor() { super('bad'); }
    transform(event) { throw new Error('fail'); }
  }

  const runner = new Runner({ store, refs, log });
  runner.register(new BadGate());
  runner.emit(new Event('bad', {}));

  const entries = log.sample().entries;
  // Entry 0: 'bad' claimed by 'bad'
  assertEqual(entries[0].type, 'bad');
  assertEqual(entries[0].claimed, 'bad');
  // Entry 1: 'error' unclaimed
  assertEqual(entries[1].type, 'error');
  assertEqual(entries[1].claimed, null);
});

// ── DATA level captures payloads ────────────────

test('Runner+Log: DATA level captures event payloads', () => {
  fresh();
  const log = new StreamLog('DATA');
  const runner = new Runner({ store, refs, log });
  runner.emit(new Event('test', { secret: 'hello' }));
  const entry = log.sample().entries[0];
  assertEqual(entry.data.secret, 'hello');
});

const exitCode = report('runner-log');
process.exit(exitCode);
