import { test, assert, assertEqual, report } from './runner.js';
import { ReadSet } from '../src/protocol/ReadSet.js';

// ── ref ─────────────────────────────────────────

test('ReadSet: ref adds a specific ref name', () => {
  const rs = new ReadSet().ref('db/tables/users/schema');
  assertEqual(rs.refs.length, 1);
  assertEqual(rs.refs[0], 'db/tables/users/schema');
});

test('ReadSet: multiple refs', () => {
  const rs = new ReadSet()
    .ref('db/tables/users/schema')
    .ref('db/tables/users/next_id');
  assertEqual(rs.refs.length, 2);
  assertEqual(rs.refs[0], 'db/tables/users/schema');
  assertEqual(rs.refs[1], 'db/tables/users/next_id');
});

// ── pattern ─────────────────────────────────────

test('ReadSet: pattern adds a prefix pattern', () => {
  const rs = new ReadSet().pattern('db/tables/users/rows/');
  assertEqual(rs.patterns.length, 1);
  assertEqual(rs.patterns[0], 'db/tables/users/rows/');
});

test('ReadSet: multiple patterns', () => {
  const rs = new ReadSet()
    .pattern('db/tables/users/rows/')
    .pattern('db/tables/users/indexes/');
  assertEqual(rs.patterns.length, 2);
});

// ── chaining ────────────────────────────────────

test('ReadSet: chaining refs and patterns', () => {
  const rs = new ReadSet()
    .ref('db/tables/users/schema')
    .ref('db/tables/users/next_id')
    .pattern('db/tables/users/indexes/');
  assertEqual(rs.refs.length, 2);
  assertEqual(rs.patterns.length, 1);
});

// ── empty ───────────────────────────────────────

test('ReadSet: empty ReadSet', () => {
  const rs = new ReadSet();
  assertEqual(rs.refs.length, 0);
  assertEqual(rs.patterns.length, 0);
});

const exitCode = report('readset');
process.exit(exitCode);
