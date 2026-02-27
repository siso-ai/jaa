import { test, assert, assertEqual, report } from './runner.js';
import { Runner } from '../src/resolution/Runner.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { MemoryRefs } from '../src/persistence/Refs.js';
import { PureGate } from '../src/protocol/PureGate.js';
import { Event } from '../src/core/Event.js';

let store, refs;

function fresh() {
  store = new MemoryStore();
  refs = new MemoryRefs();
}

// ── basic PureGate processing ───────────────────

test('Runner+Pure: registers and processes PureGates', () => {
  fresh();
  class Double extends PureGate {
    constructor() { super('double'); }
    transform(event) {
      return new Event('doubled', { value: event.data.x * 2 });
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new Double());
  runner.emit(new Event('double', { x: 5 }));
  const sample = runner.sample();
  assertEqual(sample.pending.length, 1);
  assertEqual(sample.pending[0].type, 'doubled');
  assertEqual(sample.pending[0].data.value, 10);
});

test('Runner+Pure: returning null consumes event', () => {
  fresh();
  class Drop extends PureGate {
    constructor() { super('drop'); }
    transform(event) { return null; }
  }
  const runner = new Runner({ store, refs });
  runner.register(new Drop());
  runner.emit(new Event('drop', {}));
  assertEqual(runner.sample().pending.length, 0);
});

test('Runner+Pure: chains work (A → B → pending)', () => {
  fresh();
  class A extends PureGate {
    constructor() { super('a'); }
    transform(event) { return new Event('b', { val: event.data.val + 1 }); }
  }
  class B extends PureGate {
    constructor() { super('b'); }
    transform(event) { return new Event('result', { val: event.data.val + 1 }); }
  }
  const runner = new Runner({ store, refs });
  runner.register(new A());
  runner.register(new B());
  runner.emit(new Event('a', { val: 0 }));
  const sample = runner.sample();
  assertEqual(sample.pending.length, 1);
  assertEqual(sample.pending[0].type, 'result');
  assertEqual(sample.pending[0].data.val, 2);
});

test('Runner+Pure: unclaimed events go to pending', () => {
  fresh();
  const runner = new Runner({ store, refs });
  runner.emit(new Event('nobody_home', { x: 1 }));
  const sample = runner.sample();
  assertEqual(sample.pending.length, 1);
  assertEqual(sample.pending[0].type, 'nobody_home');
});

test('Runner+Pure: multiple unclaimed events accumulate', () => {
  fresh();
  const runner = new Runner({ store, refs });
  runner.emit(new Event('a', {}));
  runner.emit(new Event('b', {}));
  runner.emit(new Event('c', {}));
  assertEqual(runner.sample().pending.length, 3);
});

test('Runner+Pure: depth-first processing preserved', () => {
  fresh();
  const order = [];
  class X extends PureGate {
    constructor() { super('x'); }
    transform(event) {
      order.push('x-start');
      const result = new Event('y', {});
      order.push('x-end');
      return result;
    }
  }
  class Y extends PureGate {
    constructor() { super('y'); }
    transform(event) {
      order.push('y');
      return null;
    }
  }
  const runner = new Runner({ store, refs });
  runner.register(new X());
  runner.register(new Y());
  runner.emit(new Event('x', {}));
  // x-start, x-end happen in X's transform, then Y runs after emit
  // But PureGate transform returns the event, wrapper emits it
  // So: wrapper calls X.transform → gets Event('y') → stream.emit('y') → Y runs
  // The order should show Y ran during X's wrapper execution
  assertEqual(order[0], 'x-start');
  assertEqual(order[1], 'x-end');
  assertEqual(order[2], 'y');
});

const exitCode = report('runner-pure');
process.exit(exitCode);
