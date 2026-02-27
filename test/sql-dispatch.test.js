import { test, assert, assertEqual, report } from './runner.js';
import { SQLDispatchGate } from '../src/gates/query/sql/SQLDispatchGate.js';
import { Event } from '../src/core/Event.js';

const gate = new SQLDispatchGate();
function dispatch(sql) { return gate.transform(new Event('sql', { sql })); }

test('Dispatch: routes CREATE TABLE', () => {
  assertEqual(dispatch('CREATE TABLE users (id INT)').type, 'create_table_parse');
});

test('Dispatch: routes INSERT', () => {
  assertEqual(dispatch("INSERT INTO users VALUES ('Alice')").type, 'insert_parse');
});

test('Dispatch: routes SELECT', () => {
  assertEqual(dispatch('SELECT * FROM users').type, 'select_parse');
});

test('Dispatch: routes UPDATE', () => {
  assertEqual(dispatch("UPDATE users SET name = 'Bob'").type, 'update_parse');
});

test('Dispatch: routes DELETE', () => {
  assertEqual(dispatch('DELETE FROM users WHERE id = 1').type, 'delete_parse');
});

test('Dispatch: routes DROP TABLE', () => {
  assertEqual(dispatch('DROP TABLE users').type, 'drop_table_parse');
});

test('Dispatch: routes CREATE INDEX', () => {
  assertEqual(dispatch('CREATE INDEX idx ON users (name)').type, 'index_create_parse');
});

test('Dispatch: routes CREATE UNIQUE INDEX', () => {
  assertEqual(dispatch('CREATE UNIQUE INDEX idx ON users (email)').type, 'index_create_parse');
});

test('Dispatch: routes DROP INDEX', () => {
  assertEqual(dispatch('DROP INDEX idx ON users').type, 'index_drop_parse');
});

test('Dispatch: routes CREATE VIEW', () => {
  assertEqual(dispatch('CREATE VIEW v AS SELECT * FROM users').type, 'view_create_parse');
});

test('Dispatch: routes DROP VIEW', () => {
  assertEqual(dispatch('DROP VIEW v').type, 'view_drop_parse');
});

test('Dispatch: routes CREATE TRIGGER', () => {
  assertEqual(dispatch('CREATE TRIGGER t AFTER INSERT ON users BEGIN END').type, 'trigger_create_parse');
});

test('Dispatch: routes DROP TRIGGER', () => {
  assertEqual(dispatch('DROP TRIGGER t').type, 'trigger_drop_parse');
});

test('Dispatch: unknown SQL emits error', () => {
  const result = dispatch('GRANT ALL ON users');
  assertEqual(result.type, 'error');
  assert(result.data.message.includes('Unrecognized'));
});

test('Dispatch: case insensitive', () => {
  assertEqual(dispatch('select * from users').type, 'select_parse');
});

const exitCode = report('sql-dispatch');
process.exit(exitCode);
