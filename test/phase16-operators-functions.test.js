/**
 * ICE Database — Phase 16 JS Test Suite
 * ILIKE, NOT BETWEEN, expanded string/math/utility functions.
 */
import { test, assert, assertEqual, report, section } from './runner.js';
import { Runner } from '../src/resolution/Runner.js';
import { MemoryStore } from '../src/persistence/Store.js';
import { MemoryRefs } from '../src/persistence/Refs.js';
import { Event } from '../src/core/Event.js';
import { registerDatabaseGates } from '../src/gates/database/register.js';
import { registerSQLGates } from '../src/gates/query/sql/register.js';

function freshRunner() {
  const r = new Runner({ store: new MemoryStore(), refs: new MemoryRefs() });
  registerDatabaseGates(r);
  registerSQLGates(r);
  return r;
}

function sql(r, query) { r.emit(new Event('sql', { sql: query })); }
function queryRows(r, query) {
  r.clearPending();
  sql(r, query);
  const results = r.sample().pending.filter(e => e.type === 'query_result');
  return results.length > 0 ? results[results.length - 1].data.rows : [];
}

function setupItems(r) {
  sql(r, 'CREATE TABLE items (name TEXT, category TEXT, price INTEGER)');
  sql(r, "INSERT INTO items (name, category, price) VALUES ('Widget', 'Tools', 25)");
  sql(r, "INSERT INTO items (name, category, price) VALUES ('Gadget', 'Electronics', 50)");
  sql(r, "INSERT INTO items (name, category, price) VALUES ('gizmo', 'electronics', 15)");
  sql(r, "INSERT INTO items (name, category, price) VALUES ('Bolt', 'Hardware', 3)");
  sql(r, "INSERT INTO items (name, category, price) VALUES ('Nut', 'Hardware', 2)");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('LIKE — case-sensitive');

test('LIKE is case-sensitive', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT name FROM items WHERE name LIKE 'G%'");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Gadget');
});

test('LIKE with underscore', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT name FROM items WHERE name LIKE '_ut'");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Nut');
});

test('NOT LIKE', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT name FROM items WHERE name NOT LIKE '%et'");
  assertEqual(rows.length, 3); // gizmo, Bolt, Nut
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('ILIKE — case-insensitive');

test('ILIKE matches case-insensitively', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT name FROM items WHERE name ILIKE 'g%'");
  assertEqual(rows.length, 2); // Gadget and gizmo
});

test('ILIKE with category', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT name FROM items WHERE category ILIKE 'electronics'");
  assertEqual(rows.length, 2);
});

test('NOT ILIKE', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT name FROM items WHERE name NOT ILIKE 'g%'");
  assertEqual(rows.length, 3); // Widget, Bolt, Nut
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('NOT BETWEEN');

test('NOT BETWEEN excludes range', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT name FROM items WHERE price NOT BETWEEN 10 AND 30 ORDER BY name");
  assertEqual(rows.length, 3); // Bolt=3, Gadget=50, Nut=2
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('String functions — LEFT, RIGHT, REVERSE, REPEAT');

test('LEFT(str, n)', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT LEFT(name, 3) AS prefix FROM items WHERE name = 'Widget'");
  assertEqual(rows[0].prefix, 'Wid');
});

test('RIGHT(str, n)', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT RIGHT(name, 3) AS suffix FROM items WHERE name = 'Widget'");
  assertEqual(rows[0].suffix, 'get');
});

test('REVERSE(str)', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT REVERSE(name) AS rev FROM items WHERE name = 'Bolt'");
  assertEqual(rows[0].rev, 'tloB');
});

test('REPEAT(str, n)', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT REPEAT(name, 2) AS doubled FROM items WHERE name = 'Nut'");
  assertEqual(rows[0].doubled, 'NutNut');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('String functions — LPAD, RPAD, POSITION');

test('LPAD(str, len, fill)', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT LPAD(name, 8, '*') AS padded FROM items WHERE name = 'Bolt'");
  assertEqual(rows[0].padded, '****Bolt');
});

test('RPAD(str, len, fill)', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT RPAD(name, 8, '.') AS padded FROM items WHERE name = 'Bolt'");
  assertEqual(rows[0].padded, 'Bolt....');
});

test('POSITION(str, substr)', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT POSITION(name, 'dg') AS pos FROM items WHERE name = 'Widget'");
  assertEqual(rows[0].pos, 3);
});

test('POSITION returns 0 when not found', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT POSITION(name, 'xyz') AS pos FROM items WHERE name = 'Widget'");
  assertEqual(rows[0].pos, 0);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('String functions — CHAR_LENGTH, STARTS_WITH, ENDS_WITH');

test('CHAR_LENGTH', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT CHAR_LENGTH(name) AS len FROM items WHERE name = 'Widget'");
  assertEqual(rows[0].len, 6);
});

test('STARTS_WITH', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT name FROM items WHERE STARTS_WITH(name, 'Ga') = 1");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Gadget');
});

test('ENDS_WITH', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT name FROM items WHERE ENDS_WITH(name, 'et') = 1");
  assertEqual(rows.length, 2); // Widget, Gadget
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Math functions — CEIL, FLOOR, POWER, SQRT');

test('CEIL and FLOOR', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE nums (val REAL)');
  sql(r, 'INSERT INTO nums (val) VALUES (3.7)');
  const rows = queryRows(r, 'SELECT CEIL(val) AS c, FLOOR(val) AS f FROM nums');
  assertEqual(rows[0].c, 4);
  assertEqual(rows[0].f, 3);
});

test('POWER and SQRT', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE nums (val INTEGER)');
  sql(r, 'INSERT INTO nums (val) VALUES (9)');
  const rows = queryRows(r, 'SELECT POWER(val, 2) AS sq, SQRT(val) AS rt FROM nums');
  assertEqual(rows[0].sq, 81);
  assertEqual(rows[0].rt, 3);
});

test('MOD', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE nums (val INTEGER)');
  sql(r, 'INSERT INTO nums (val) VALUES (17)');
  const rows = queryRows(r, 'SELECT MOD(val, 5) AS m FROM nums');
  assertEqual(rows[0].m, 2);
});

test('SIGN', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE nums (val INTEGER)');
  sql(r, 'INSERT INTO nums (val) VALUES (-5)');
  sql(r, 'INSERT INTO nums (val) VALUES (0)');
  sql(r, 'INSERT INTO nums (val) VALUES (7)');
  const rows = queryRows(r, 'SELECT val, SIGN(val) AS s FROM nums ORDER BY val');
  assertEqual(rows[0].s, -1);
  assertEqual(rows[1].s, 0);
  assertEqual(rows[2].s, 1);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Math functions — LOG, LN, EXP, PI');

test('LN and EXP', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE nums (val REAL)');
  sql(r, 'INSERT INTO nums (val) VALUES (1)');
  const rows = queryRows(r, 'SELECT LN(val) AS l, EXP(val) AS e FROM nums');
  assertEqual(rows[0].l, 0);
  assert(Math.abs(rows[0].e - 2.718) < 0.01);
});

test('PI()', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE nums (val INTEGER)');
  sql(r, 'INSERT INTO nums (val) VALUES (1)');
  const rows = queryRows(r, 'SELECT PI() AS pi FROM nums');
  assert(Math.abs(rows[0].pi - 3.14159) < 0.001);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Utility functions — TYPEOF, GREATEST, LEAST');

test('TYPEOF', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT TYPEOF(name) AS tn, TYPEOF(price) AS tp FROM items WHERE name = 'Widget'");
  assertEqual(rows[0].tn, 'text');
  assertEqual(rows[0].tp, 'integer');
});

test('GREATEST', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE nums (a INTEGER, b INTEGER, c INTEGER)');
  sql(r, 'INSERT INTO nums (a, b, c) VALUES (10, 30, 20)');
  const rows = queryRows(r, 'SELECT GREATEST(a, b, c) AS mx FROM nums');
  assertEqual(rows[0].mx, 30);
});

test('LEAST', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE nums (a INTEGER, b INTEGER, c INTEGER)');
  sql(r, 'INSERT INTO nums (a, b, c) VALUES (10, 30, 20)');
  const rows = queryRows(r, 'SELECT LEAST(a, b, c) AS mn FROM nums');
  assertEqual(rows[0].mn, 10);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Integration');

test('ILIKE + string function in SELECT', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, "SELECT UPPER(name) AS n, LPAD(CAST(price AS TEXT), 5, '0') AS p FROM items WHERE category ILIKE 'hardware' ORDER BY name");
  assertEqual(rows.length, 2);
  assertEqual(rows[0].n, 'BOLT');
  assertEqual(rows[0].p, '00003');
});

test('BETWEEN + CEIL in expression', () => {
  const r = freshRunner(); setupItems(r);
  const rows = queryRows(r, 'SELECT name, CEIL(price * 1.1) AS tax_price FROM items WHERE price BETWEEN 10 AND 30 ORDER BY name');
  assertEqual(rows.length, 2); // Widget=25, gizmo=15
});

test('NOT BETWEEN + GREATEST', () => {
  const r = freshRunner();
  sql(r, 'CREATE TABLE scores (name TEXT, math INTEGER, english INTEGER, science INTEGER)');
  sql(r, "INSERT INTO scores (name, math, english, science) VALUES ('Alice', 85, 92, 78)");
  sql(r, "INSERT INTO scores (name, math, english, science) VALUES ('Bob', 60, 55, 70)");
  const rows = queryRows(r, "SELECT name, GREATEST(math, english, science) AS best FROM scores WHERE GREATEST(math, english, science) NOT BETWEEN 70 AND 80");
  assertEqual(rows.length, 1);
  assertEqual(rows[0].name, 'Alice');
  assertEqual(rows[0].best, 92);
});

report('phase16-operators-functions');
