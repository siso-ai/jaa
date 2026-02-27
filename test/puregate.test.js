import { test, assert, assertEqual, report } from './runner.js';
import { PureGate } from '../src/protocol/PureGate.js';
import { Gate } from '../src/core/Gate.js';
import { Event } from '../src/core/Event.js';

// ── signature ───────────────────────────────────

test('PureGate: has a signature', () => {
  class Echo extends PureGate {
    constructor() { super('echo'); }
    transform(event) { return new Event('echoed', event.data); }
  }
  const gate = new Echo();
  assertEqual(gate.signature, 'echo');
});

// ── transform ───────────────────────────────────

test('PureGate: transform returns an Event', () => {
  class Upper extends PureGate {
    constructor() { super('upper'); }
    transform(event) {
      return new Event('uppered', { text: event.data.text.toUpperCase() });
    }
  }
  const gate = new Upper();
  const result = gate.transform(new Event('upper', { text: 'hello' }));
  assertEqual(result.type, 'uppered');
  assertEqual(result.data.text, 'HELLO');
});

test('PureGate: transform can return null (consume and drop)', () => {
  class BlackHole extends PureGate {
    constructor() { super('drop'); }
    transform(event) { return null; }
  }
  const result = new BlackHole().transform(new Event('drop', {}));
  assertEqual(result, null);
});

test('PureGate: transform preserves complex data', () => {
  class PassThrough extends PureGate {
    constructor() { super('pass'); }
    transform(event) {
      return new Event('passed', {
        items: event.data.items.map(i => i * 2),
        meta: event.data.meta
      });
    }
  }
  const gate = new PassThrough();
  const result = gate.transform(new Event('pass', {
    items: [1, 2, 3],
    meta: { source: 'test' }
  }));
  assertEqual(result.data.items[0], 2);
  assertEqual(result.data.items[2], 6);
  assertEqual(result.data.meta.source, 'test');
});

// ── inheritance ─────────────────────────────────

test('PureGate: extends Gate', () => {
  const gate = new PureGate('test');
  assert(gate instanceof Gate);
});

test('PureGate: base transform returns null', () => {
  const gate = new PureGate('base');
  const result = gate.transform(new Event('base', {}));
  assertEqual(result, null);
});

const exitCode = report('puregate');
process.exit(exitCode);
