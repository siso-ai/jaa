import { test, assert, assertEqual, report } from './runner.js';
import { Runner } from '../src/resolution/Runner.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { MemoryRefs } from '../src/persistence/Refs.js';
import { registerDatabaseGates } from '../src/gates/database/register.js';
import { registerSQLGates } from '../src/gates/query/sql/register.js';
import { Event } from '../src/core/Event.js';

let store, refs, runner;

function fresh() {
  store = new MemoryStore();
  refs = new MemoryRefs();
  runner = new Runner({ store, refs });
  registerDatabaseGates(runner);
  registerSQLGates(runner);
}

function sql(statement) {
  runner.emit(new Event('sql', { sql: statement }));
}

function lastResult() {
  const pending = runner.sample().pending;
  const results = pending.filter(e => e.type === 'query_result');
  return results[results.length - 1];
}

// ── CREATE + INSERT + SELECT ────────────────────

test('E2E: CREATE TABLE via SQL', () => {
  fresh();
  sql('CREATE TABLE users (id INTEGER NOT NULL, name TEXT, age INTEGER)');
  assert(refs.get('db/tables/users/schema') !== null, 'schema exists');
});

test('E2E: INSERT via SQL', () => {
  fresh();
  sql('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)');
  sql("INSERT INTO users (name, age) VALUES ('Alice', 30)");
  sql("INSERT INTO users (name, age) VALUES ('Bob', 25)");

  const row1 = store.get(refs.get('db/tables/users/rows/1'));
  assertEqual(row1.name, 'Alice');
  assertEqual(row1.age, 30);

  const row2 = store.get(refs.get('db/tables/users/rows/2'));
  assertEqual(row2.name, 'Bob');
});

test('E2E: SELECT * FROM table', () => {
  fresh();
  sql('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)');
  sql("INSERT INTO users (name, age) VALUES ('Alice', 30)");
  sql("INSERT INTO users (name, age) VALUES ('Bob', 25)");

  sql('SELECT * FROM users');
  const result = lastResult();
  assert(result !== undefined, 'query_result emitted');
  assertEqual(result.data.rows.length, 2);
});

// ── SELECT with WHERE ───────────────────────────

test('E2E: SELECT with WHERE filters correctly', () => {
  fresh();
  sql('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)');
  sql("INSERT INTO users (name, age) VALUES ('Alice', 30)");
  sql("INSERT INTO users (name, age) VALUES ('Bob', 25)");
  sql("INSERT INTO users (name, age) VALUES ('Carol', 35)");

  sql('SELECT * FROM users WHERE age > 28');
  const result = lastResult();
  assertEqual(result.data.rows.length, 2);
  assert(result.data.rows.every(r => r.age > 28));
});

// ── SELECT with ORDER BY ────────────────────────

test('E2E: SELECT with ORDER BY sorts correctly', () => {
  fresh();
  sql('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)');
  sql("INSERT INTO users (name, age) VALUES ('Alice', 30)");
  sql("INSERT INTO users (name, age) VALUES ('Bob', 25)");
  sql("INSERT INTO users (name, age) VALUES ('Carol', 35)");

  sql('SELECT * FROM users ORDER BY age ASC');
  const result = lastResult();
  assertEqual(result.data.rows[0].age, 25);
  assertEqual(result.data.rows[1].age, 30);
  assertEqual(result.data.rows[2].age, 35);
});

// ── SELECT with LIMIT ───────────────────────────

test('E2E: SELECT with LIMIT limits correctly', () => {
  fresh();
  sql('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)');
  sql("INSERT INTO users (name, age) VALUES ('Alice', 30)");
  sql("INSERT INTO users (name, age) VALUES ('Bob', 25)");
  sql("INSERT INTO users (name, age) VALUES ('Carol', 35)");

  sql('SELECT * FROM users ORDER BY age ASC LIMIT 2');
  const result = lastResult();
  assertEqual(result.data.rows.length, 2);
  assertEqual(result.data.rows[0].age, 25);
  assertEqual(result.data.rows[1].age, 30);
});

// ── UPDATE via SQL ──────────────────────────────

test('E2E: UPDATE via SQL', () => {
  fresh();
  sql('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)');
  sql("INSERT INTO users (name, age) VALUES ('Alice', 30)");

  sql("UPDATE users SET age = 31 WHERE name = 'Alice'");

  const row = store.get(refs.get('db/tables/users/rows/1'));
  assertEqual(row.age, 31);
});

// ── DELETE via SQL ──────────────────────────────

test('E2E: DELETE via SQL', () => {
  fresh();
  sql('CREATE TABLE users (id INTEGER, name TEXT, age INTEGER)');
  sql("INSERT INTO users (name, age) VALUES ('Alice', 30)");
  sql("INSERT INTO users (name, age) VALUES ('Bob', 25)");

  sql("DELETE FROM users WHERE name = 'Bob'");

  assertEqual(refs.get('db/tables/users/rows/2'), null);
  assert(refs.get('db/tables/users/rows/1') !== null);
});

// ── DROP TABLE via SQL ──────────────────────────

test('E2E: DROP TABLE via SQL', () => {
  fresh();
  sql('CREATE TABLE temp (id INTEGER)');
  assert(refs.get('db/tables/temp/schema') !== null);

  sql('DROP TABLE temp');
  assertEqual(refs.get('db/tables/temp/schema'), null);
});

// ── SELECT with JOIN ────────────────────────────

test('E2E: SELECT with JOIN', () => {
  fresh();
  sql('CREATE TABLE users (id INTEGER, name TEXT, dept_id INTEGER)');
  sql('CREATE TABLE depts (id INTEGER, dept_name TEXT)');
  sql("INSERT INTO users (name, dept_id) VALUES ('Alice', 1)");
  sql("INSERT INTO users (name, dept_id) VALUES ('Bob', 2)");
  sql("INSERT INTO depts (dept_name) VALUES ('Engineering')");
  sql("INSERT INTO depts (dept_name) VALUES ('Sales')");

  sql('SELECT * FROM users JOIN depts ON dept_id = id');
  const result = lastResult();
  assert(result !== undefined, 'join result exists');
  assertEqual(result.data.rows.length, 2);
  assert(result.data.rows.some(r => r.name === 'Alice' && r.dept_name === 'Engineering'));
});

// ── SELECT with GROUP BY ────────────────────────

test('E2E: SELECT with GROUP BY and COUNT', () => {
  fresh();
  sql('CREATE TABLE users (id INTEGER, name TEXT, dept TEXT)');
  sql("INSERT INTO users (name, dept) VALUES ('Alice', 'eng')");
  sql("INSERT INTO users (name, dept) VALUES ('Bob', 'sales')");
  sql("INSERT INTO users (name, dept) VALUES ('Carol', 'eng')");

  sql('SELECT dept, COUNT(*) AS cnt FROM users GROUP BY dept');
  const result = lastResult();
  assert(result !== undefined, 'aggregate result exists');
  const eng = result.data.rows.find(r => r.dept === 'eng');
  assertEqual(eng.cnt, 2);
});

// ── Full lifecycle via SQL ──────────────────────

test('E2E: full SQL lifecycle', () => {
  fresh();
  sql('CREATE TABLE products (id INTEGER, name TEXT, price REAL, in_stock BOOLEAN)');
  sql("INSERT INTO products (name, price, in_stock) VALUES ('Widget', 9.99, TRUE)");
  sql("INSERT INTO products (name, price, in_stock) VALUES ('Gadget', 24.99, TRUE)");
  sql("INSERT INTO products (name, price, in_stock) VALUES ('Doohickey', 4.99, FALSE)");

  // Query
  sql('SELECT * FROM products WHERE price > 5 ORDER BY price DESC');
  let result = lastResult();
  assertEqual(result.data.rows.length, 2);
  assertEqual(result.data.rows[0].name, 'Gadget');

  // Update
  sql("UPDATE products SET in_stock = TRUE WHERE name = 'Doohickey'");

  // Delete
  sql("DELETE FROM products WHERE name = 'Widget'");

  // Verify final state
  sql('SELECT * FROM products ORDER BY price ASC');
  result = lastResult();
  assertEqual(result.data.rows.length, 2);
  assertEqual(result.data.rows[0].name, 'Doohickey');
});

const exitCode = report('query-plan-e2e');
process.exit(exitCode);
