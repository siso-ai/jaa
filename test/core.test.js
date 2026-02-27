import { test, assert, assertEqual, assertThrows, report } from './runner.js';
import { Event } from '../src/core/Event.js';
import { Gate } from '../src/core/Gate.js';
import { Stream } from '../src/core/Stream.js';

// ── Event ──────────────────────────────────────

test('Event: type and data', () => {
  const e = new Event('foo', { x: 1 });
  assertEqual(e.type, 'foo');
  assertEqual(e.data.x, 1);
});

test('Event: data defaults to empty object', () => {
  const e = new Event('bar');
  assertEqual(e.type, 'bar');
  assert(typeof e.data === 'object');
  assertEqual(Object.keys(e.data).length, 0);
});

// ── Gate ───────────────────────────────────────

test('Gate: signature stored', () => {
  const g = new Gate('test_sig');
  assertEqual(g.signature, 'test_sig');
});

test('Gate: transform is a no-op by default', () => {
  const g = new Gate('noop');
  // Should not throw
  g.transform(new Event('noop'), {});
});

// ── Stream: registration ───────────────────────

test('Stream: register and lookup', () => {
  const s = new Stream();
  const g = new Gate('x');
  s.register(g);
  assert(s.gates.has('x'));
});

test('Stream: signature collision throws', () => {
  const s = new Stream();
  s.register(new Gate('x'));
  assertThrows(() => s.register(new Gate('x')), 'Signature collision');
});

test('Stream: different signatures coexist', () => {
  const s = new Stream();
  s.register(new Gate('a'));
  s.register(new Gate('b'));
  assertEqual(s.gates.size, 2);
});

// ── Stream: emit and pending ───────────────────

test('Stream: unclaimed event goes to pending', () => {
  const s = new Stream();
  s.emit(new Event('unknown', { v: 42 }));
  const sample = s.sampleHere();
  assertEqual(sample.pending.length, 1);
  assertEqual(sample.pending[0].type, 'unknown');
  assertEqual(sample.pending[0].data.v, 42);
});

test('Stream: claimed event does not go to pending', () => {
  class Sink extends Gate {
    constructor() { super('sink'); }
    transform() {}
  }
  const s = new Stream();
  s.register(new Sink());
  s.emit(new Event('sink', {}));
  assertEqual(s.sampleHere().pending.length, 0);
});

test('Stream: gate transform receives event and stream', () => {
  let received = null;
  class Spy extends Gate {
    constructor() { super('spy'); }
    transform(event, stream) {
      received = { type: event.type, data: event.data, hasEmit: typeof stream.emit === 'function' };
    }
  }
  const s = new Stream();
  s.register(new Spy());
  s.emit(new Event('spy', { val: 99 }));
  assertEqual(received.type, 'spy');
  assertEqual(received.data.val, 99);
  assert(received.hasEmit);
});

test('Stream: gate can emit into same stream', () => {
  class Doubler extends Gate {
    constructor() { super('double'); }
    transform(event, stream) {
      stream.emit(new Event('result', { value: event.data.x * 2 }));
    }
  }
  const s = new Stream();
  s.register(new Doubler());
  s.emit(new Event('double', { x: 5 }));
  const sample = s.sampleHere();
  assertEqual(sample.pending.length, 1);
  assertEqual(sample.pending[0].data.value, 10);
});

test('Stream: depth-first processing', () => {
  const order = [];
  class A extends Gate {
    constructor() { super('a'); }
    transform(event, stream) {
      order.push('a-start');
      stream.emit(new Event('b', {}));
      order.push('a-end');
    }
  }
  class B extends Gate {
    constructor() { super('b'); }
    transform(event, stream) {
      order.push('b');
    }
  }
  const s = new Stream();
  s.register(new A());
  s.register(new B());
  s.emit(new Event('a', {}));
  assertEqual(order.join(','), 'a-start,b,a-end');
});

// ── Stream: sampleHere ─────────────────────────

test('Stream: sampleHere returns copy of pending', () => {
  const s = new Stream();
  s.emit(new Event('x', {}));
  const sample = s.sampleHere();
  sample.pending.pop();
  assertEqual(s.sampleHere().pending.length, 1, 'original not affected');
});

test('Stream: eventCount tracks all emits', () => {
  class Echo extends Gate {
    constructor() { super('echo'); }
    transform(event, stream) {
      stream.emit(new Event('out', {}));
    }
  }
  const s = new Stream();
  s.register(new Echo());
  s.emit(new Event('echo', {}));
  assertEqual(s.sampleHere().eventCount, 2);
});

test('Stream: gateCount in sampleHere', () => {
  const s = new Stream();
  s.register(new Gate('a'));
  s.register(new Gate('b'));
  s.register(new Gate('c'));
  assertEqual(s.sampleHere().gateCount, 3);
});

test('Stream: pending accumulates across emits', () => {
  const s = new Stream();
  s.emit(new Event('x', {}));
  s.emit(new Event('y', {}));
  assertEqual(s.sampleHere().pending.length, 2);
});

// ── Stream: constructor options ────────────────

test('Stream: no-arg constructor still works', () => {
  const s = new Stream();
  assertEqual(s.log, null);
  assertEqual(s.streamId, null);
  assertEqual(s.parentStreamId, null);
});

test('Stream: constructor accepts log', () => {
  const fakeLog = { nextStreamId: () => 42 };
  const s = new Stream({ log: fakeLog });
  assertEqual(s.log, fakeLog);
  assertEqual(s.streamId, 42);
});

const exitCode = report('core');
process.exit(exitCode);
