import { test, assert, assertEqual, report } from './runner.js';
import { StreamLog } from '../src/core/StreamLog.js';
import { Stream } from '../src/core/Stream.js';
import { Event } from '../src/core/Event.js';
import { Gate } from '../src/core/Gate.js';

// ── Levels ─────────────────────────────────────

test('StreamLog: OFF records nothing', () => {
  const log = new StreamLog('OFF');
  log.record({ streamId: 1, eventType: 'x', gateClaimed: 'x', eventData: {} });
  assertEqual(log.sample().count, 0);
});

test('StreamLog: EVENTS records type and claimed', () => {
  const log = new StreamLog('EVENTS');
  log.record({ streamId: 1, eventType: 'foo', gateClaimed: 'foo', eventData: { big: true } });
  const entries = log.sample().entries;
  assertEqual(entries.length, 1);
  assertEqual(entries[0].type, 'foo');
  assertEqual(entries[0].claimed, 'foo');
  assert(entries[0].streamId === undefined, 'no streamId at EVENTS');
  assert(entries[0].data === undefined, 'no data at EVENTS');
});

test('StreamLog: EVENTS records null claimed for pending', () => {
  const log = new StreamLog('EVENTS');
  log.record({ streamId: 1, eventType: 'orphan', gateClaimed: null, eventData: {} });
  assertEqual(log.sample().entries[0].claimed, null);
});

test('StreamLog: DEEP includes streamId and parentStreamId', () => {
  const log = new StreamLog('DEEP');
  log.record({ streamId: 5, parentStreamId: 3, eventType: 'x', gateClaimed: 'x', eventData: {} });
  const e = log.sample().entries[0];
  assertEqual(e.streamId, 5);
  assertEqual(e.parentStreamId, 3);
  assert(e.data === undefined, 'no data at DEEP');
});

test('StreamLog: DEEP omits parentStreamId when null', () => {
  const log = new StreamLog('DEEP');
  log.record({ streamId: 1, parentStreamId: null, eventType: 'x', gateClaimed: null, eventData: {} });
  assert(!('parentStreamId' in log.sample().entries[0]));
});

test('StreamLog: DATA includes everything', () => {
  const log = new StreamLog('DATA');
  log.record({ streamId: 2, parentStreamId: 1, eventType: 'y', gateClaimed: 'y', eventData: { val: 42 } });
  const e = log.sample().entries[0];
  assertEqual(e.streamId, 2);
  assertEqual(e.parentStreamId, 1);
  assertEqual(e.data.val, 42);
});

// ── Sequencing ─────────────────────────────────

test('StreamLog: seq increments', () => {
  const log = new StreamLog('EVENTS');
  log.record({ streamId: 1, eventType: 'a', gateClaimed: null, eventData: {} });
  log.record({ streamId: 1, eventType: 'b', gateClaimed: null, eventData: {} });
  log.record({ streamId: 1, eventType: 'c', gateClaimed: null, eventData: {} });
  const entries = log.sample().entries;
  assertEqual(entries[0].seq, 0);
  assertEqual(entries[1].seq, 1);
  assertEqual(entries[2].seq, 2);
});

test('StreamLog: entries have timestamps', () => {
  const log = new StreamLog('EVENTS');
  const before = Date.now();
  log.record({ streamId: 1, eventType: 'x', gateClaimed: null, eventData: {} });
  const after = Date.now();
  const time = log.sample().entries[0].time;
  assert(time >= before && time <= after);
});

// ── Stream IDs ─────────────────────────────────

test('StreamLog: nextStreamId increments', () => {
  const log = new StreamLog('EVENTS');
  const a = log.nextStreamId();
  const b = log.nextStreamId();
  assertEqual(b, a + 1);
});

// ── sample and clear ───────────────────────────

test('StreamLog: sample returns copy', () => {
  const log = new StreamLog('EVENTS');
  log.record({ streamId: 1, eventType: 'x', gateClaimed: null, eventData: {} });
  const s = log.sample();
  s.entries.pop();
  assertEqual(log.sample().count, 1);
});

test('StreamLog: clear resets entries and seq', () => {
  const log = new StreamLog('EVENTS');
  log.record({ streamId: 1, eventType: 'x', gateClaimed: null, eventData: {} });
  log.record({ streamId: 1, eventType: 'y', gateClaimed: null, eventData: {} });
  log.clear();
  assertEqual(log.sample().count, 0);
  log.record({ streamId: 1, eventType: 'z', gateClaimed: null, eventData: {} });
  assertEqual(log.sample().entries[0].seq, 0, 'seq resets');
});

// ── Runtime level change ───────────────────────

test('StreamLog: level change takes effect immediately', () => {
  const log = new StreamLog('OFF');
  log.record({ streamId: 1, eventType: 'a', gateClaimed: null, eventData: {} });
  assertEqual(log.sample().count, 0);

  log.level = 'EVENTS';
  log.record({ streamId: 1, eventType: 'b', gateClaimed: null, eventData: {} });
  assertEqual(log.sample().count, 1);

  log.level = 'OFF';
  log.record({ streamId: 1, eventType: 'c', gateClaimed: null, eventData: {} });
  assertEqual(log.sample().count, 1);
});

test('StreamLog: upgrade level mid-run adds detail', () => {
  const log = new StreamLog('EVENTS');
  log.record({ streamId: 1, eventType: 'a', gateClaimed: null, eventData: { x: 1 } });

  log.level = 'DATA';
  log.record({ streamId: 1, eventType: 'b', gateClaimed: null, eventData: { x: 2 } });

  const entries = log.sample().entries;
  assert(entries[0].data === undefined, 'first entry has no data');
  assertEqual(entries[1].data.x, 2, 'second entry has data');
});

// ── Integration with Stream ────────────────────

test('StreamLog: Stream.emit records to log', () => {
  const log = new StreamLog('EVENTS');
  class Sink extends Gate {
    constructor() { super('sink'); }
    transform() {}
  }
  const s = new Stream({ log });
  s.register(new Sink());
  s.emit(new Event('sink', {}));
  s.emit(new Event('orphan', {}));

  const entries = log.sample().entries;
  assertEqual(entries.length, 2);
  assertEqual(entries[0].claimed, 'sink');
  assertEqual(entries[1].claimed, null);
});

test('StreamLog: shared log across parent and sub-stream', () => {
  const log = new StreamLog('DEEP');
  class Spawner extends Gate {
    constructor() { super('spawn'); }
    transform(event, stream) {
      const sub = new Stream({ log, parentStreamId: stream.streamId });
      sub.emit(new Event('child_event', {}));
    }
  }
  const parent = new Stream({ log });
  parent.register(new Spawner());
  parent.emit(new Event('spawn', {}));

  const entries = log.sample().entries;
  assertEqual(entries.length, 2);
  assertEqual(entries[0].streamId, parent.streamId);
  assertEqual(entries[1].parentStreamId, parent.streamId);
});

const exitCode = report('streamlog');
process.exit(exitCode);
