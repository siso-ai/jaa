import { test, assert, assertEqual, report } from './runner.js';
import { tokenize } from '../src/gates/query/sql/tokenizer.js';
import { CreateTableParseGate } from '../src/gates/query/sql/CreateTableParseGate.js';
import { DropTableParseGate } from '../src/gates/query/sql/DropTableParseGate.js';
import { InsertParseGate } from '../src/gates/query/sql/InsertParseGate.js';
import { UpdateParseGate } from '../src/gates/query/sql/UpdateParseGate.js';
import { DeleteParseGate } from '../src/gates/query/sql/DeleteParseGate.js';
import { Event } from '../src/core/Event.js';

function parse(gate, sql) {
  const tokens = tokenize(sql);
  return gate.transform(new Event(gate.signature, { sql, tokens }));
}

// ── CREATE TABLE ────────────────────────────────

const ct = new CreateTableParseGate();

test('Parse CREATE TABLE: basic', () => {
  const r = parse(ct, 'CREATE TABLE users (id INTEGER, name TEXT)');
  assertEqual(r.type, 'create_table_execute');
  assertEqual(r.data.table, 'users');
  assertEqual(r.data.columns.length, 2);
  assertEqual(r.data.columns[0].name, 'id');
  assertEqual(r.data.columns[0].type, 'integer');
  assertEqual(r.data.columns[1].name, 'name');
  assertEqual(r.data.columns[1].type, 'text');
});

test('Parse CREATE TABLE: NOT NULL', () => {
  const r = parse(ct, 'CREATE TABLE t (id INTEGER NOT NULL)');
  assertEqual(r.data.columns[0].nullable, false);
});

test('Parse CREATE TABLE: DEFAULT value', () => {
  const r = parse(ct, "CREATE TABLE t (status TEXT DEFAULT 'active')");
  assertEqual(r.data.columns[0].default, 'active');
});

test('Parse CREATE TABLE: mixed constraints', () => {
  const r = parse(ct, "CREATE TABLE t (id INTEGER NOT NULL, name TEXT DEFAULT 'anon', age REAL)");
  assertEqual(r.data.columns[0].nullable, false);
  assertEqual(r.data.columns[1].default, 'anon');
  assertEqual(r.data.columns[2].type, 'real');
  assertEqual(r.data.columns[2].nullable, true);
});

test('Parse CREATE TABLE: type normalization', () => {
  const r = parse(ct, 'CREATE TABLE t (a INT, b VARCHAR, c FLOAT, d BOOL)');
  assertEqual(r.data.columns[0].type, 'integer');
  assertEqual(r.data.columns[1].type, 'text');
  assertEqual(r.data.columns[2].type, 'real');
  assertEqual(r.data.columns[3].type, 'boolean');
});

// ── DROP TABLE ──────────────────────────────────

const dt = new DropTableParseGate();

test('Parse DROP TABLE: basic', () => {
  const r = parse(dt, 'DROP TABLE users');
  assertEqual(r.type, 'drop_table_execute');
  assertEqual(r.data.table, 'users');
  assertEqual(r.data.ifExists, false);
});

test('Parse DROP TABLE: IF EXISTS', () => {
  const r = parse(dt, 'DROP TABLE IF EXISTS users');
  assertEqual(r.data.table, 'users');
  assertEqual(r.data.ifExists, true);
});

// ── INSERT ──────────────────────────────────────

const ins = new InsertParseGate();

test('Parse INSERT: named columns', () => {
  const r = parse(ins, "INSERT INTO users (name, age) VALUES ('Alice', 30)");
  assertEqual(r.type, 'insert_execute');
  assertEqual(r.data.table, 'users');
  assertEqual(r.data.row.name, 'Alice');
  assertEqual(r.data.row.age, 30);
});

test('Parse INSERT: without column names', () => {
  const r = parse(ins, "INSERT INTO users VALUES ('Alice', 30)");
  assertEqual(r.type, 'insert_execute');
  assertEqual(r.data.table, 'users');
  // Positional columns
  assertEqual(r.data.row._col0, 'Alice');
  assertEqual(r.data.row._col1, 30);
});

test('Parse INSERT: NULL value', () => {
  const r = parse(ins, 'INSERT INTO users (name, age) VALUES (NULL, 25)');
  assertEqual(r.data.row.name, null);
  assertEqual(r.data.row.age, 25);
});

test('Parse INSERT: boolean values', () => {
  const r = parse(ins, 'INSERT INTO flags (active) VALUES (TRUE)');
  assertEqual(r.data.row.active, true);
});

// ── UPDATE ──────────────────────────────────────

const up = new UpdateParseGate();

test('Parse UPDATE: SET and WHERE', () => {
  const r = parse(up, "UPDATE users SET age = 31 WHERE name = 'Alice'");
  assertEqual(r.type, 'update_execute');
  assertEqual(r.data.table, 'users');
  assertEqual(r.data.changes.age, 31);
  assertEqual(r.data.where.column, 'name');
  assertEqual(r.data.where.op, '=');
  assertEqual(r.data.where.value, 'Alice');
});

test('Parse UPDATE: multiple columns', () => {
  const r = parse(up, "UPDATE users SET age = 31, name = 'Bob' WHERE id = 1");
  assertEqual(r.data.changes.age, 31);
  assertEqual(r.data.changes.name, 'Bob');
});

test('Parse UPDATE: without WHERE', () => {
  const r = parse(up, 'UPDATE users SET age = 0');
  assertEqual(r.data.changes.age, 0);
  assertEqual(r.data.where, null);
});

// ── DELETE ──────────────────────────────────────

const del = new DeleteParseGate();

test('Parse DELETE: with WHERE', () => {
  const r = parse(del, 'DELETE FROM users WHERE age < 18');
  assertEqual(r.type, 'delete_execute');
  assertEqual(r.data.table, 'users');
  assertEqual(r.data.where.column, 'age');
  assertEqual(r.data.where.op, '<');
  assertEqual(r.data.where.value, 18);
});

test('Parse DELETE: without WHERE', () => {
  const r = parse(del, 'DELETE FROM users');
  assertEqual(r.data.table, 'users');
  assertEqual(r.data.where, null);
});

const exitCode = report('parse-ddl-dml');
process.exit(exitCode);
