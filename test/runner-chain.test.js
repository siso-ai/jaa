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

// ── PureGate → StateGate chain ──────────────────

test('Runner+Chain: PureGate → StateGate (parse then execute)', () => {
  fresh();

  // Pre-populate schema and counter
  const schemaHash = store.put({ name: 'users', columns: ['id', 'name'] });
  refs.set('db/tables/users/schema', schemaHash);
  const counterHash = store.put('0');
  refs.set('db/tables/users/next_id', counterHash);

  class ParseInsert extends PureGate {
    constructor() { super('insert_sql'); }
    transform(event) {
      return new Event('insert_execute', {
        table: 'users',
        row: { name: 'Alice' }
      });
    }
  }

  class ExecuteInsert extends StateGate {
    constructor() { super('insert_execute'); }
    reads(event) {
      return new ReadSet()
        .ref(`db/tables/${event.data.table}/schema`)
        .ref(`db/tables/${event.data.table}/next_id`);
    }
    transform(event, state) {
      const counter = parseInt(state.refs[`db/tables/${event.data.table}/next_id`]) || 0;
      const id = counter + 1;
      const row = { id, ...event.data.row };
      return new MutationBatch()
        .put('row', row)
        .refSet(`db/tables/${event.data.table}/rows/${id}`, 0)
        .put('counter', String(id))
        .refSet(`db/tables/${event.data.table}/next_id`, 1)
        .emit(new Event('insert_result', { table: event.data.table, id }));
    }
  }

  const runner = new Runner({ store, refs });
  runner.register(new ParseInsert());
  runner.register(new ExecuteInsert());
  runner.emit(new Event('insert_sql', {}));

  // Row should be in store
  const rowHash = refs.get('db/tables/users/rows/1');
  assert(rowHash !== null, 'row ref should exist');
  assertEqual(store.get(rowHash).name, 'Alice');
  assertEqual(store.get(rowHash).id, 1);

  // Counter should be updated
  const newCounterHash = refs.get('db/tables/users/next_id');
  assertEqual(store.get(newCounterHash), '1');

  // Follow-up event emitted
  const result = runner.sample().pending[0];
  assertEqual(result.type, 'insert_result');
  assertEqual(result.data.id, 1);
});

// ── StateGate → PureGate chain ──────────────────

test('Runner+Chain: StateGate → PureGate (execute then format)', () => {
  fresh();

  class CreateThing extends StateGate {
    constructor() { super('create'); }
    reads(event) { return new ReadSet(); }
    transform(event, state) {
      return new MutationBatch()
        .put('thing', { name: event.data.name })
        .refSet(`things/${event.data.name}`, 0)
        .emit(new Event('format_result', { name: event.data.name, status: 'created' }));
    }
  }

  class FormatResult extends PureGate {
    constructor() { super('format_result'); }
    transform(event) {
      return new Event('display', {
        message: `${event.data.name}: ${event.data.status}`
      });
    }
  }

  const runner = new Runner({ store, refs });
  runner.register(new CreateThing());
  runner.register(new FormatResult());
  runner.emit(new Event('create', { name: 'widget' }));

  // Mutation applied
  assert(refs.get('things/widget') !== null);

  // Chain completed: create → format_result → display
  const pending = runner.sample().pending;
  assertEqual(pending.length, 1);
  assertEqual(pending[0].type, 'display');
  assertEqual(pending[0].data.message, 'widget: created');
});

// ── StateGate → StateGate chain ─────────────────

test('Runner+Chain: StateGate → StateGate (create then insert)', () => {
  fresh();

  class CreateTable extends StateGate {
    constructor() { super('create_table'); }
    reads(event) { return new ReadSet(); }
    transform(event, state) {
      return new MutationBatch()
        .put('schema', { name: event.data.table, columns: event.data.columns })
        .refSet(`db/tables/${event.data.table}/schema`, 0)
        .put('counter', '0')
        .refSet(`db/tables/${event.data.table}/next_id`, 1)
        .emit(new Event('auto_insert', { table: event.data.table, row: event.data.seedRow }));
    }
  }

  class AutoInsert extends StateGate {
    constructor() { super('auto_insert'); }
    reads(event) {
      return new ReadSet()
        .ref(`db/tables/${event.data.table}/next_id`);
    }
    transform(event, state) {
      const counter = parseInt(state.refs[`db/tables/${event.data.table}/next_id`]) || 0;
      const id = counter + 1;
      const row = { id, ...event.data.row };
      return new MutationBatch()
        .put('row', row)
        .refSet(`db/tables/${event.data.table}/rows/${id}`, 0)
        .put('counter', String(id))
        .refSet(`db/tables/${event.data.table}/next_id`, 1)
        .emit(new Event('inserted', { table: event.data.table, id }));
    }
  }

  const runner = new Runner({ store, refs });
  runner.register(new CreateTable());
  runner.register(new AutoInsert());
  runner.emit(new Event('create_table', {
    table: 'logs',
    columns: ['id', 'message'],
    seedRow: { message: 'table created' }
  }));

  // Schema exists
  assert(refs.get('db/tables/logs/schema') !== null);

  // Seed row was inserted (second StateGate read the state
  // that the first StateGate wrote — depth-first, same stream)
  const rowHash = refs.get('db/tables/logs/rows/1');
  assert(rowHash !== null, 'seed row should exist');
  assertEqual(store.get(rowHash).message, 'table created');
  assertEqual(store.get(rowHash).id, 1);

  // Counter updated to "1"
  assertEqual(store.get(refs.get('db/tables/logs/next_id')), '1');
});

// ── Three-gate chain ────────────────────────────

test('Runner+Chain: three gates (Pure → State → Pure)', () => {
  fresh();

  class Parse extends PureGate {
    constructor() { super('raw'); }
    transform(event) {
      return new Event('store_it', { key: event.data.key, value: event.data.value });
    }
  }

  class StoreIt extends StateGate {
    constructor() { super('store_it'); }
    reads(event) { return new ReadSet(); }
    transform(event, state) {
      return new MutationBatch()
        .put('data', event.data.value)
        .refSet(event.data.key, 0)
        .emit(new Event('confirm', { key: event.data.key }));
    }
  }

  class Confirm extends PureGate {
    constructor() { super('confirm'); }
    transform(event) {
      return new Event('done', { message: `stored ${event.data.key}` });
    }
  }

  const runner = new Runner({ store, refs });
  runner.register(new Parse());
  runner.register(new StoreIt());
  runner.register(new Confirm());
  runner.emit(new Event('raw', { key: 'mykey', value: { x: 42 } }));

  // Value in store
  assertEqual(store.get(refs.get('mykey')).x, 42);

  // Full chain completed
  const pending = runner.sample().pending;
  assertEqual(pending.length, 1);
  assertEqual(pending[0].type, 'done');
  assertEqual(pending[0].data.message, 'stored mykey');
});

const exitCode = report('runner-chain');
process.exit(exitCode);
