import { test, assert, assertEqual, report } from './runner.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { MemoryRefs } from '../src/persistence/Refs.js';

let store;
let refs;

function fresh() {
  store = new MemoryStore();
  refs = new MemoryRefs();
}

// ── store + ref round trip ──────────────────────

test('Integration: store an object, ref it, retrieve by name', () => {
  fresh();
  const hash = store.put({ name: 'users', columns: ['id', 'name'] });
  refs.set('db/tables/users/schema', hash);
  const retrieved = store.get(refs.get('db/tables/users/schema'));
  assertEqual(retrieved.name, 'users');
  assertEqual(retrieved.columns[0], 'id');
  assertEqual(retrieved.columns[1], 'name');
});

// ── update pattern ──────────────────────────────

test('Integration: update pattern — new object, swing ref', () => {
  fresh();
  const h1 = store.put({ val: 1 });
  refs.set('counter', h1);
  const h2 = store.put({ val: 2 });
  refs.set('counter', h2);

  // Current ref points to new value
  assertEqual(store.get(refs.get('counter')).val, 2);
  // Old object still in store
  assertEqual(store.get(h1).val, 1);
});

// ── delete ref, object persists ─────────────────

test('Integration: delete ref, object persists in store', () => {
  fresh();
  const hash = store.put({ data: 'important' });
  refs.set('temp', hash);
  refs.delete('temp');
  assertEqual(refs.get('temp'), null);
  assertEqual(store.get(hash).data, 'important');
});

// ── table simulation ────────────────────────────

test('Integration: simulate a table with rows', () => {
  fresh();

  // Create schema
  const schemaHash = store.put({
    name: 'users',
    columns: [
      { name: 'id', type: 'integer' },
      { name: 'name', type: 'text' },
      { name: 'age', type: 'integer' }
    ]
  });
  refs.set('db/tables/users/schema', schemaHash);

  // Create counter
  const counterHash = store.put('0');
  refs.set('db/tables/users/next_id', counterHash);

  // Insert rows
  const row1Hash = store.put({ id: 1, name: 'Alice', age: 30 });
  refs.set('db/tables/users/rows/1', row1Hash);
  refs.set('db/tables/users/next_id', store.put('1'));

  const row2Hash = store.put({ id: 2, name: 'Bob', age: 25 });
  refs.set('db/tables/users/rows/2', row2Hash);
  refs.set('db/tables/users/next_id', store.put('2'));

  const row3Hash = store.put({ id: 3, name: 'Carol', age: 35 });
  refs.set('db/tables/users/rows/3', row3Hash);
  refs.set('db/tables/users/next_id', store.put('3'));

  // Scan: list all row refs, resolve each
  const rowRefs = refs.list('db/tables/users/rows/');
  assertEqual(rowRefs.length, 3);

  const rows = rowRefs.map(ref => store.get(refs.get(ref)));
  assertEqual(rows.length, 3);

  const alice = rows.find(r => r.name === 'Alice');
  assertEqual(alice.age, 30);

  // Update: new object, swing ref
  const updatedAlice = { ...alice, age: 31 };
  const updatedHash = store.put(updatedAlice);
  refs.set('db/tables/users/rows/1', updatedHash);

  // Old Alice still in store
  assertEqual(store.get(row1Hash).age, 30);
  // New Alice via ref
  assertEqual(store.get(refs.get('db/tables/users/rows/1')).age, 31);

  // Delete: remove ref, object persists
  refs.delete('db/tables/users/rows/2');
  assertEqual(refs.list('db/tables/users/rows/').length, 2);
  assertEqual(store.get(row2Hash).name, 'Bob'); // still in store
});

// ── cross-domain: refs are just strings ─────────

test('Integration: multiple domains share store and refs', () => {
  fresh();

  // Database row
  const rowHash = store.put({ id: 1, name: 'Alice' });
  refs.set('db/tables/users/rows/1', rowHash);

  // Git commit
  const commitHash = store.put({ message: 'initial commit', author: 'Alice' });
  refs.set('git/commits/abc123', commitHash);

  // Wiki page
  const pageHash = store.put({ title: 'Home', content: 'Welcome' });
  refs.set('wiki/pages/home', pageHash);

  // All in one store, all accessible by name
  assertEqual(store.get(refs.get('db/tables/users/rows/1')).name, 'Alice');
  assertEqual(store.get(refs.get('git/commits/abc123')).message, 'initial commit');
  assertEqual(store.get(refs.get('wiki/pages/home')).title, 'Home');

  // Store has 3 objects
  assertEqual(store.objects.size, 3);
});

const exitCode = report('persistence-integration');
process.exit(exitCode);
