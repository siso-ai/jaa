import { test, assert, assertEqual, report } from './runner.js';
import { tokenize } from '../src/gates/query/sql/tokenizer.js';

test('Tokenizer: simple SELECT', () => {
  const tokens = tokenize('SELECT name FROM users');
  assertEqual(tokens[0].type, 'KEYWORD');
  assertEqual(tokens[0].value, 'SELECT');
  assertEqual(tokens[1].type, 'IDENTIFIER');
  assertEqual(tokens[1].value, 'name');
  assertEqual(tokens[2].type, 'KEYWORD');
  assertEqual(tokens[2].value, 'FROM');
  assertEqual(tokens[3].type, 'IDENTIFIER');
  assertEqual(tokens[3].value, 'users');
});

test('Tokenizer: string literals', () => {
  const tokens = tokenize("INSERT INTO t VALUES ('hello')");
  const str = tokens.find(t => t.type === 'STRING');
  assertEqual(str.value, 'hello');
});

test('Tokenizer: escaped quotes in strings', () => {
  const tokens = tokenize("VALUES ('it''s')");
  const str = tokens.find(t => t.type === 'STRING');
  assertEqual(str.value, "it's");
});

test('Tokenizer: numbers (integer and float)', () => {
  const tokens = tokenize('WHERE age > 21 AND score = 3.14');
  const nums = tokens.filter(t => t.type === 'NUMBER');
  assertEqual(nums[0].value, 21);
  assertEqual(nums[1].value, 3.14);
});

test('Tokenizer: operators', () => {
  const tokens = tokenize('WHERE x >= 10 AND y != 20 AND z <> 30');
  const ops = tokens.filter(t => t.type === 'OPERATOR');
  assertEqual(ops[0].value, '>=');
  assertEqual(ops[1].value, '!=');
  assertEqual(ops[2].value, '<>');
});

test('Tokenizer: symbols', () => {
  const tokens = tokenize('SELECT * FROM (t)');
  const syms = tokens.filter(t => t.type === 'SYMBOL');
  assert(syms.some(s => s.value === '*'));
  assert(syms.some(s => s.value === '('));
  assert(syms.some(s => s.value === ')'));
});

test('Tokenizer: case-insensitive keywords', () => {
  const tokens = tokenize('select NAME from USERS');
  assertEqual(tokens[0].type, 'KEYWORD');
  assertEqual(tokens[0].value, 'SELECT');
  assertEqual(tokens[1].type, 'IDENTIFIER');
  assertEqual(tokens[1].value, 'NAME');
});

test('Tokenizer: quoted identifiers', () => {
  const tokens = tokenize('SELECT "column name" FROM t');
  assertEqual(tokens[1].type, 'IDENTIFIER');
  assertEqual(tokens[1].value, 'column name');
});

test('Tokenizer: backtick identifiers', () => {
  const tokens = tokenize('SELECT `col` FROM t');
  assertEqual(tokens[1].type, 'IDENTIFIER');
  assertEqual(tokens[1].value, 'col');
});

test('Tokenizer: boolean and null', () => {
  const tokens = tokenize('VALUES (TRUE, FALSE, NULL)');
  assertEqual(tokens[2].type, 'BOOLEAN');
  assertEqual(tokens[2].value, true);
  assertEqual(tokens[4].type, 'BOOLEAN');
  assertEqual(tokens[4].value, false);
  assertEqual(tokens[6].type, 'NULL');
  assertEqual(tokens[6].value, null);
});

test('Tokenizer: empty string', () => {
  assertEqual(tokenize('').length, 0);
});

test('Tokenizer: handles SQL comments', () => {
  const tokens = tokenize('SELECT * -- this is a comment\nFROM t');
  assertEqual(tokens[0].value, 'SELECT');
  assertEqual(tokens[2].value, 'FROM');
});

test('Tokenizer: complex CREATE TABLE', () => {
  const tokens = tokenize("CREATE TABLE users (id INTEGER NOT NULL, name TEXT DEFAULT 'unknown')");
  assert(tokens.some(t => t.value === 'CREATE'));
  assert(tokens.some(t => t.value === 'INTEGER'));
  assert(tokens.some(t => t.value === 'DEFAULT'));
  const str = tokens.find(t => t.type === 'STRING');
  assertEqual(str.value, 'unknown');
});

const exitCode = report('tokenizer');
process.exit(exitCode);
