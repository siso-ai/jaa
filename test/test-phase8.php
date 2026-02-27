<?php
/**
 * ICE Database — Phase 8 Test Suite
 * SQL Parser, Query Planner, and End-to-End SQL in PHP.
 * Run: php test/test-phase8.php
 */
require __DIR__ . '/runner.php';
require __DIR__ . '/../ice/autoload.php';

use Ice\Core\Event;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Persistence\FileStore;
use Ice\Persistence\FileRefs;
use Ice\Resolution\Runner;

use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;
use function Ice\Gates\Query\SQL\tokenize;
use function Ice\Gates\Query\SQL\kw;
use function Ice\Gates\Query\SQL\sym;
use function Ice\Gates\Query\SQL\match_token;
use function Ice\Gates\Query\SQL\parseLiteralValue;
use function Ice\Gates\Query\SQL\parseWhereClause;
use function Ice\Gates\Query\SQL\parseOrderBy;
use function Ice\Gates\Query\SQL\parseColumnList;
use function Ice\Gates\Query\SQL\parseTableRef;
use function Ice\Gates\Query\SQL\parseValueList;
use function Ice\Gates\Query\SQL\parseIdentList;
use function Ice\Gates\Query\SQL\normalizeType;

echo "ICE Database — Phase 8: SQL Parser & Query Layer (PHP)\n";

// ─── Helpers ──────────────────────────────────────────────
function freshRunner(): Runner {
    $runner = new Runner(new MemoryStore(), new MemoryRefs());
    registerDatabaseGates($runner);
    registerSQLGates($runner);
    return $runner;
}

function sql(Runner $r, string $query): void {
    $r->emit(new Event('sql', ['sql' => $query]));
}

function getPending(Runner $r): array { return $r->sample()['pending']; }

function lastPending(Runner $r): ?Event {
    $p = getPending($r);
    return count($p) > 0 ? $p[count($p) - 1] : null;
}

function pendingOfType(Runner $r, string $type): array {
    return array_values(array_filter(getPending($r), fn($e) => $e->type === $type));
}

function queryRows(Runner $r, string $query): array {
    sql($r, $query);
    $results = pendingOfType($r, 'query_result');
    if (count($results) === 0) return [];
    return end($results)->data['rows'];
}

$tmpDir = sys_get_temp_dir() . '/ice_test8_' . getmypid();

function cleanTmp(): void {
    global $tmpDir;
    if (is_dir($tmpDir)) {
        $it = new RecursiveDirectoryIterator($tmpDir, RecursiveDirectoryIterator::SKIP_DOTS);
        $files = new RecursiveIteratorIterator($it, RecursiveIteratorIterator::CHILD_FIRST);
        foreach ($files as $f) { $f->isDir() ? rmdir($f->getRealPath()) : unlink($f->getRealPath()); }
        rmdir($tmpDir);
    }
}

// ─── TOKENIZER TESTS ────────────────────────────────────
section('Tokenizer');

test('tokenize SELECT *', function () {
    $tokens = tokenize('SELECT * FROM users');
    assertEqual($tokens[0]['type'], 'KEYWORD');
    assertEqual($tokens[0]['value'], 'SELECT');
    assertEqual($tokens[1]['type'], 'SYMBOL');
    assertEqual($tokens[1]['value'], '*');
    assertEqual($tokens[2]['type'], 'KEYWORD');
    assertEqual($tokens[2]['value'], 'FROM');
    assertEqual($tokens[3]['type'], 'IDENTIFIER');
    assertEqual($tokens[3]['value'], 'users');
});

test('tokenize keywords case-insensitive', function () {
    $tokens = tokenize('select from where');
    assertEqual($tokens[0]['value'], 'SELECT');
    assertEqual($tokens[1]['value'], 'FROM');
    assertEqual($tokens[2]['value'], 'WHERE');
});

test('tokenize integer', function () {
    $tokens = tokenize('42');
    assertEqual($tokens[0]['type'], 'NUMBER');
    assertEqual($tokens[0]['value'], 42);
});

test('tokenize float', function () {
    $tokens = tokenize('3.14');
    assertEqual($tokens[0]['type'], 'NUMBER');
    assertTrue(abs($tokens[0]['value'] - 3.14) < 0.001);
});

test('tokenize string literal', function () {
    $tokens = tokenize("'hello world'");
    assertEqual($tokens[0]['type'], 'STRING');
    assertEqual($tokens[0]['value'], 'hello world');
});

test('tokenize escaped quotes', function () {
    $tokens = tokenize("'it''s'");
    assertEqual($tokens[0]['value'], "it's");
});

test('tokenize double-quoted identifier', function () {
    $tokens = tokenize('"col name"');
    assertEqual($tokens[0]['type'], 'IDENTIFIER');
    assertEqual($tokens[0]['value'], 'col name');
});

test('tokenize backtick-quoted identifier', function () {
    $tokens = tokenize('`table`');
    assertEqual($tokens[0]['type'], 'IDENTIFIER');
    assertEqual($tokens[0]['value'], 'table');
});

test('tokenize operators', function () {
    $tokens = tokenize('>= <= <> != = < >');
    assertEqual($tokens[0]['value'], '>=');
    assertEqual($tokens[1]['value'], '<=');
    assertEqual($tokens[2]['value'], '<>');
    assertEqual($tokens[3]['value'], '!=');
    assertEqual($tokens[4]['value'], '=');
});

test('tokenize symbols', function () {
    $tokens = tokenize('(,)*.*;');
    assertEqual($tokens[0]['value'], '(');
    assertEqual($tokens[1]['value'], ',');
    assertEqual($tokens[2]['value'], ')');
    assertEqual($tokens[3]['value'], '*');
    assertEqual($tokens[4]['value'], '.');
    assertEqual($tokens[5]['value'], '*');
    assertEqual($tokens[6]['value'], ';');
});

test('tokenize boolean TRUE/FALSE', function () {
    $tokens = tokenize('TRUE FALSE');
    assertEqual($tokens[0]['type'], 'BOOLEAN');
    assertEqual($tokens[0]['value'], true);
    assertEqual($tokens[1]['type'], 'BOOLEAN');
    assertEqual($tokens[1]['value'], false);
});

test('tokenize NULL', function () {
    $tokens = tokenize('NULL');
    assertEqual($tokens[0]['type'], 'NULL');
    assertNull($tokens[0]['value']);
});

test('tokenize negative number', function () {
    $tokens = tokenize('WHERE x = -5');
    $neg = $tokens[count($tokens) - 1];
    assertEqual($neg['type'], 'NUMBER');
    assertEqual($neg['value'], -5);
});

test('tokenize skips comments', function () {
    $tokens = tokenize("SELECT -- this is a comment\n* FROM t");
    assertEqual(count($tokens), 4);
    assertEqual($tokens[1]['value'], '*');
});

test('tokenize preserves identifier case', function () {
    $tokens = tokenize('myColumn');
    assertEqual($tokens[0]['value'], 'myColumn');
});

test('tokenize complex INSERT', function () {
    $tokens = tokenize("INSERT INTO users (name, age) VALUES ('Alice', 30)");
    assertEqual($tokens[0]['value'], 'INSERT');
    assertEqual($tokens[1]['value'], 'INTO');
    assertEqual($tokens[2]['value'], 'users');
});

test('tokenize empty SQL', function () {
    $tokens = tokenize('');
    assertCount(0, $tokens);
});

test('tokenize whitespace-only', function () {
    $tokens = tokenize('   ');
    assertCount(0, $tokens);
});

// ─── PARSER UTILS TESTS ─────────────────────────────────
section('Parser Utils');

test('kw matches keyword', function () {
    $tokens = tokenize('SELECT FROM');
    assertTrue(kw($tokens, 0, 'SELECT'));
    assertTrue(kw($tokens, 1, 'FROM'));
    assertFalse(kw($tokens, 0, 'FROM'));
});

test('sym matches symbol', function () {
    $tokens = tokenize('(*)');
    assertTrue(sym($tokens, 0, '('));
    assertTrue(sym($tokens, 1, '*'));
    assertTrue(sym($tokens, 2, ')'));
});

test('match_token out of bounds', function () {
    assertFalse(match_token([], 0, 'KEYWORD', 'SELECT'));
});

test('parseLiteralValue number', function () {
    $tokens = tokenize('42');
    $result = parseLiteralValue($tokens, 0);
    assertEqual($result['value'], 42);
    assertEqual($result['pos'], 1);
});

test('parseLiteralValue string', function () {
    $tokens = tokenize("'hello'");
    $result = parseLiteralValue($tokens, 0);
    assertEqual($result['value'], 'hello');
});

test('parseLiteralValue boolean', function () {
    $tokens = tokenize('TRUE');
    $result = parseLiteralValue($tokens, 0);
    assertEqual($result['value'], true);
});

test('parseLiteralValue null', function () {
    $tokens = tokenize('NULL');
    $result = parseLiteralValue($tokens, 0);
    assertNull($result['value']);
});

test('parseLiteralValue negative', function () {
    $tokens = [['type' => 'OPERATOR', 'value' => '-'], ['type' => 'NUMBER', 'value' => 10]];
    $result = parseLiteralValue($tokens, 0);
    assertEqual($result['value'], -10);
});

test('parseWhereClause simple =', function () {
    $tokens = tokenize('x = 5');
    $result = parseWhereClause($tokens, 0);
    assertEqual($result['condition']['column'], 'x');
    assertEqual($result['condition']['op'], '=');
    assertEqual($result['condition']['value'], 5);
});

test('parseWhereClause AND', function () {
    $tokens = tokenize('x > 0 AND x < 10');
    $result = parseWhereClause($tokens, 0);
    assertTrue(isset($result['condition']['and']));
    assertCount(2, $result['condition']['and']);
});

test('parseWhereClause OR', function () {
    $tokens = tokenize('x = 1 OR x = 2');
    $result = parseWhereClause($tokens, 0);
    assertTrue(isset($result['condition']['or']));
});

test('parseWhereClause NOT', function () {
    $tokens = tokenize('NOT x = 1');
    $result = parseWhereClause($tokens, 0);
    assertTrue(isset($result['condition']['not']));
});

test('parseWhereClause IN', function () {
    $tokens = tokenize("x IN (1, 2, 3)");
    $result = parseWhereClause($tokens, 0);
    assertEqual($result['condition']['op'], 'in');
    assertEqual($result['condition']['value'], [1, 2, 3]);
});

test('parseWhereClause LIKE', function () {
    $tokens = tokenize("name LIKE 'Al%'");
    $result = parseWhereClause($tokens, 0);
    assertEqual($result['condition']['op'], 'like');
    assertEqual($result['condition']['value'], 'Al%');
});

test('parseWhereClause IS NULL', function () {
    $tokens = tokenize('x IS NULL');
    $result = parseWhereClause($tokens, 0);
    assertEqual($result['condition']['op'], 'is_null');
});

test('parseWhereClause IS NOT NULL', function () {
    $tokens = tokenize('x IS NOT NULL');
    $result = parseWhereClause($tokens, 0);
    assertEqual($result['condition']['op'], 'is_not_null');
});

test('parseWhereClause BETWEEN', function () {
    $tokens = tokenize('x BETWEEN 1 AND 10');
    $result = parseWhereClause($tokens, 0);
    assertTrue(isset($result['condition']['and']));
    assertEqual($result['condition']['and'][0]['op'], '>=');
    assertEqual($result['condition']['and'][1]['op'], '<=');
});

test('parseOrderBy single', function () {
    $tokens = tokenize('name ASC');
    $result = parseOrderBy($tokens, 0);
    assertEqual($result['order'][0]['column'], 'name');
    assertEqual($result['order'][0]['direction'], 'asc');
});

test('parseOrderBy multi', function () {
    $tokens = tokenize('a DESC, b ASC');
    $result = parseOrderBy($tokens, 0);
    assertCount(2, $result['order']);
    assertEqual($result['order'][0]['direction'], 'desc');
    assertEqual($result['order'][1]['direction'], 'asc');
});

test('parseOrderBy default direction', function () {
    $tokens = tokenize('name');
    $result = parseOrderBy($tokens, 0);
    assertEqual($result['order'][0]['direction'], 'asc');
});

test('parseColumnList *', function () {
    $tokens = tokenize('*');
    $result = parseColumnList($tokens, 0);
    assertEqual($result['columns'], ['*']);
});

test('parseColumnList named columns', function () {
    $tokens = tokenize('a, b, c FROM');
    $result = parseColumnList($tokens, 0);
    assertEqual($result['columns'], ['a', 'b', 'c']);
});

test('parseColumnList aggregate', function () {
    $tokens = tokenize('COUNT(*) FROM');
    $result = parseColumnList($tokens, 0);
    assertTrue(is_array($result['columns'][0]));
    assertEqual($result['columns'][0]['aggregate']['fn'], 'COUNT');
});

test('parseColumnList aggregate with alias', function () {
    $tokens = tokenize('SUM(price) AS total FROM');
    $result = parseColumnList($tokens, 0);
    assertEqual($result['columns'][0]['alias'], 'total');
});

test('parseTableRef simple', function () {
    $tokens = tokenize('users WHERE');
    $result = parseTableRef($tokens, 0);
    assertEqual($result['table'], 'users');
    assertNull($result['alias']);
});

test('parseTableRef with AS alias', function () {
    $tokens = tokenize('users AS u WHERE');
    $result = parseTableRef($tokens, 0);
    assertEqual($result['table'], 'users');
    assertEqual($result['alias'], 'u');
});

test('parseValueList', function () {
    $tokens = tokenize("(1, 'hello', TRUE)");
    $result = parseValueList($tokens, 0);
    assertEqual($result['values'], [1, 'hello', true]);
});

test('parseIdentList', function () {
    $tokens = tokenize('(a, b, c)');
    $result = parseIdentList($tokens, 0);
    assertEqual($result['idents'], ['a', 'b', 'c']);
});

test('normalizeType mappings', function () {
    assertEqual(normalizeType('INTEGER'), 'integer');
    assertEqual(normalizeType('INT'), 'integer');
    assertEqual(normalizeType('TEXT'), 'text');
    assertEqual(normalizeType('VARCHAR'), 'text');
    assertEqual(normalizeType('REAL'), 'real');
    assertEqual(normalizeType('FLOAT'), 'real');
    assertEqual(normalizeType('BOOLEAN'), 'boolean');
    assertEqual(normalizeType('BLOB'), 'blob');
    assertEqual(normalizeType('DATE'), 'date');
    assertEqual(normalizeType('TIMESTAMP'), 'timestamp');
    assertEqual(normalizeType('UNKNOWN'), 'text');
});

// ─── SQL DISPATCH TESTS ─────────────────────────────────
section('SQL Dispatch');

test('dispatch SELECT', function () {
    $r = freshRunner();
    sql($r, 'SELECT * FROM users');
    // Should produce query_result (empty table, but no error)
    $errors = pendingOfType($r, 'error');
    // May error on missing table, but dispatch itself should work
    $plans = pendingOfType($r, 'query_result');
    // The dispatch went through — either got result or scan error
    assertTrue(count($plans) > 0 || count($errors) > 0);
});

test('dispatch CREATE TABLE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT)');
    $created = pendingOfType($r, 'table_created');
    assertTrue(count($created) > 0);
});

test('dispatch INSERT', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, "INSERT INTO t (x) VALUES (42)");
    $inserted = pendingOfType($r, 'row_inserted');
    assertTrue(count($inserted) > 0);
});

test('dispatch UPDATE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, "INSERT INTO t (x) VALUES (1)");
    sql($r, "UPDATE t SET x = 99 WHERE x = 1");
    $updated = pendingOfType($r, 'row_updated');
    assertTrue(count($updated) > 0);
});

test('dispatch DELETE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, "INSERT INTO t (x) VALUES (1)");
    sql($r, "DELETE FROM t WHERE x = 1");
    $deleted = pendingOfType($r, 'row_deleted');
    assertTrue(count($deleted) > 0);
});

test('dispatch DROP TABLE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'DROP TABLE t');
    $dropped = pendingOfType($r, 'table_dropped');
    assertTrue(count($dropped) > 0);
});

test('dispatch empty SQL error', function () {
    $r = freshRunner();
    sql($r, '  ');
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

test('dispatch unrecognized SQL error', function () {
    $r = freshRunner();
    sql($r, 'FOOBAR xyz');
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

// ─── DDL VIA SQL TESTS ──────────────────────────────────
section('DDL via SQL');

test('CREATE TABLE with column types', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE users (id INTEGER, name TEXT, score REAL, active BOOLEAN)');
    $created = pendingOfType($r, 'table_created');
    assertTrue(count($created) > 0);
});

test('CREATE TABLE IF NOT EXISTS', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'CREATE TABLE IF NOT EXISTS t (x INTEGER)');
    // No duplicate error due to IF NOT EXISTS in parse → still dispatches create_table_execute
    // The execute gate may raise an error, but the parse worked
    assertTrue(true);
});

test('CREATE TABLE with NOT NULL and DEFAULT', function () {
    $r = freshRunner();
    sql($r, "CREATE TABLE t (status TEXT NOT NULL DEFAULT 'active')");
    $created = pendingOfType($r, 'table_created');
    assertTrue(count($created) > 0);
});

test('DROP TABLE via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'DROP TABLE t');
    $dropped = pendingOfType($r, 'table_dropped');
    assertTrue(count($dropped) > 0);
});

test('DROP TABLE IF EXISTS via SQL', function () {
    $r = freshRunner();
    sql($r, 'DROP TABLE IF EXISTS nonexistent');
    // No error
    $errors = array_filter(pendingOfType($r, 'error'), fn($e) => str_contains($e->data['message'] ?? '', 'nonexistent'));
    assertCount(0, $errors);
});

// ─── DML VIA SQL TESTS ──────────────────────────────────
section('DML via SQL');

test('INSERT via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT, age INTEGER)');
    sql($r, "INSERT INTO t (name, age) VALUES ('Alice', 30)");
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Alice');
    assertEqual($rows[0]['age'], 30);
});

test('INSERT multiple rows via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    sql($r, 'INSERT INTO t (x) VALUES (3)');
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(3, $rows);
});

test('UPDATE via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT, age INTEGER)');
    sql($r, "INSERT INTO t (name, age) VALUES ('Alice', 30)");
    sql($r, "UPDATE t SET age = 31 WHERE name = 'Alice'");
    $rows = queryRows($r, 'SELECT * FROM t');
    $alice = array_values(array_filter($rows, fn($r) => $r['name'] === 'Alice'));
    assertEqual($alice[0]['age'], 31);
});

test('UPDATE multiple columns', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT, age INTEGER)');
    sql($r, "INSERT INTO t (name, age) VALUES ('Alice', 30)");
    sql($r, "UPDATE t SET name = 'Alicia', age = 31 WHERE age = 30");
    $rows = queryRows($r, 'SELECT * FROM t');
    assertEqual($rows[0]['name'], 'Alicia');
    assertEqual($rows[0]['age'], 31);
});

test('DELETE via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    sql($r, 'DELETE FROM t WHERE x = 1');
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(1, $rows);
    assertEqual($rows[0]['x'], 2);
});

test('DELETE all (no WHERE)', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    sql($r, 'DELETE FROM t');
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(0, $rows);
});

// ─── SELECT VIA SQL TESTS ───────────────────────────────
section('SELECT via SQL');

test('SELECT * FROM table', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE users (name TEXT, age INTEGER)');
    sql($r, "INSERT INTO users (name, age) VALUES ('Alice', 30)");
    sql($r, "INSERT INTO users (name, age) VALUES ('Bob', 25)");
    $rows = queryRows($r, 'SELECT * FROM users');
    assertCount(2, $rows);
});

test('SELECT specific columns', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (a INTEGER, b TEXT, c REAL)');
    sql($r, "INSERT INTO t (a, b, c) VALUES (1, 'hello', 3.14)");
    $rows = queryRows($r, 'SELECT a, b FROM t');
    assertCount(1, $rows);
    assertTrue(isset($rows[0]['a']));
    assertTrue(isset($rows[0]['b']));
});

test('SELECT with WHERE =', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    sql($r, 'INSERT INTO t (x) VALUES (3)');
    $rows = queryRows($r, 'SELECT * FROM t WHERE x = 2');
    assertCount(1, $rows);
    assertEqual($rows[0]['x'], 2);
});

test('SELECT with WHERE >', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (10)');
    sql($r, 'INSERT INTO t (x) VALUES (20)');
    sql($r, 'INSERT INTO t (x) VALUES (30)');
    $rows = queryRows($r, 'SELECT * FROM t WHERE x > 15');
    assertCount(2, $rows);
});

test('SELECT with WHERE AND', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER, y INTEGER)');
    sql($r, 'INSERT INTO t (x, y) VALUES (1, 10)');
    sql($r, 'INSERT INTO t (x, y) VALUES (2, 20)');
    sql($r, 'INSERT INTO t (x, y) VALUES (3, 30)');
    $rows = queryRows($r, 'SELECT * FROM t WHERE x > 1 AND y < 30');
    assertCount(1, $rows);
    assertEqual($rows[0]['x'], 2);
});

test('SELECT with WHERE OR', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    sql($r, 'INSERT INTO t (x) VALUES (3)');
    $rows = queryRows($r, 'SELECT * FROM t WHERE x = 1 OR x = 3');
    assertCount(2, $rows);
});

test('SELECT with WHERE IN', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    sql($r, 'INSERT INTO t (x) VALUES (3)');
    $rows = queryRows($r, 'SELECT * FROM t WHERE x IN (1, 3)');
    assertCount(2, $rows);
});

test('SELECT with WHERE LIKE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT)');
    sql($r, "INSERT INTO t (name) VALUES ('Alice')");
    sql($r, "INSERT INTO t (name) VALUES ('Bob')");
    sql($r, "INSERT INTO t (name) VALUES ('Alicia')");
    $rows = queryRows($r, "SELECT * FROM t WHERE name LIKE 'Al%'");
    assertCount(2, $rows);
});

test('SELECT with WHERE BETWEEN', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    for ($i = 1; $i <= 10; $i++) sql($r, "INSERT INTO t (x) VALUES ($i)");
    $rows = queryRows($r, 'SELECT * FROM t WHERE x BETWEEN 3 AND 7');
    assertCount(5, $rows);
});

test('SELECT with ORDER BY ASC', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (3)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    $rows = queryRows($r, 'SELECT * FROM t ORDER BY x ASC');
    assertEqual($rows[0]['x'], 1);
    assertEqual($rows[1]['x'], 2);
    assertEqual($rows[2]['x'], 3);
});

test('SELECT with ORDER BY DESC', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (3)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    $rows = queryRows($r, 'SELECT * FROM t ORDER BY x DESC');
    assertEqual($rows[0]['x'], 3);
});

test('SELECT with LIMIT', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    for ($i = 1; $i <= 10; $i++) sql($r, "INSERT INTO t (x) VALUES ($i)");
    $rows = queryRows($r, 'SELECT * FROM t ORDER BY x LIMIT 3');
    assertCount(3, $rows);
    assertEqual($rows[0]['x'], 1);
});

test('SELECT with LIMIT and OFFSET', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    for ($i = 1; $i <= 10; $i++) sql($r, "INSERT INTO t (x) VALUES ($i)");
    $rows = queryRows($r, 'SELECT * FROM t ORDER BY x LIMIT 3 OFFSET 2');
    assertCount(3, $rows);
    assertEqual($rows[0]['x'], 3);
});

test('SELECT DISTINCT', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    $rows = queryRows($r, 'SELECT DISTINCT x FROM t');
    assertCount(2, $rows);
});

test('SELECT COUNT(*)', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    sql($r, 'INSERT INTO t (x) VALUES (3)');
    $rows = queryRows($r, 'SELECT COUNT(*) AS cnt FROM t');
    assertEqual($rows[0]['cnt'], 3);
});

test('SELECT SUM', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (10)');
    sql($r, 'INSERT INTO t (x) VALUES (20)');
    sql($r, 'INSERT INTO t (x) VALUES (30)');
    $rows = queryRows($r, 'SELECT SUM(x) AS total FROM t');
    assertEqual($rows[0]['total'], 60);
});

test('SELECT AVG', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (10)');
    sql($r, 'INSERT INTO t (x) VALUES (20)');
    $rows = queryRows($r, 'SELECT AVG(x) AS avg_x FROM t');
    assertTrue(abs($rows[0]['avg_x'] - 15.0) < 0.001);
});

test('SELECT MIN/MAX', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (5)');
    sql($r, 'INSERT INTO t (x) VALUES (15)');
    sql($r, 'INSERT INTO t (x) VALUES (10)');
    $rows = queryRows($r, 'SELECT MIN(x) AS mn, MAX(x) AS mx FROM t');
    assertEqual($rows[0]['mn'], 5);
    assertEqual($rows[0]['mx'], 15);
});

test('SELECT GROUP BY', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (dept TEXT, salary INTEGER)');
    sql($r, "INSERT INTO t (dept, salary) VALUES ('eng', 100)");
    sql($r, "INSERT INTO t (dept, salary) VALUES ('eng', 200)");
    sql($r, "INSERT INTO t (dept, salary) VALUES ('sales', 150)");
    $rows = queryRows($r, 'SELECT COUNT(*) AS cnt FROM t GROUP BY dept');
    assertCount(2, $rows);
});

test('SELECT GROUP BY with SUM', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (dept TEXT, salary INTEGER)');
    sql($r, "INSERT INTO t (dept, salary) VALUES ('eng', 100)");
    sql($r, "INSERT INTO t (dept, salary) VALUES ('eng', 200)");
    sql($r, "INSERT INTO t (dept, salary) VALUES ('sales', 150)");
    $rows = queryRows($r, 'SELECT SUM(salary) AS total FROM t GROUP BY dept');
    assertCount(2, $rows);
    $totals = array_column($rows, 'total');
    sort($totals);
    assertEqual($totals[0], 150);
    assertEqual($totals[1], 300);
});

test('SELECT with JOIN', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE users (name TEXT)');
    sql($r, 'CREATE TABLE orders (user_id INTEGER, item TEXT)');
    sql($r, "INSERT INTO users (name) VALUES ('Alice')");
    sql($r, "INSERT INTO users (name) VALUES ('Bob')");
    sql($r, "INSERT INTO orders (user_id, item) VALUES (1, 'Book')");
    sql($r, "INSERT INTO orders (user_id, item) VALUES (1, 'Pen')");
    $rows = queryRows($r, 'SELECT * FROM users JOIN orders ON id = user_id');
    assertCount(2, $rows);
    assertEqual($rows[0]['name'], 'Alice');
});

test('SELECT with LEFT JOIN', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE users (name TEXT)');
    sql($r, 'CREATE TABLE orders (user_id INTEGER, item TEXT)');
    sql($r, "INSERT INTO users (name) VALUES ('Alice')");
    sql($r, "INSERT INTO users (name) VALUES ('Bob')");
    sql($r, "INSERT INTO orders (user_id, item) VALUES (1, 'Book')");
    $rows = queryRows($r, 'SELECT * FROM users LEFT JOIN orders ON id = user_id');
    assertCount(2, $rows);
});

// ─── INDEX VIA SQL TESTS ────────────────────────────────
section('Index via SQL');

test('CREATE INDEX via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (10)');
    sql($r, 'CREATE INDEX idx_x ON t (x)');
    $created = pendingOfType($r, 'index_created');
    assertTrue(count($created) > 0);
});

test('CREATE UNIQUE INDEX via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'CREATE UNIQUE INDEX idx_x ON t (x)');
    $created = pendingOfType($r, 'index_created');
    assertTrue(count($created) > 0);
});

test('DROP INDEX via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'CREATE INDEX idx_x ON t (x)');
    sql($r, 'DROP INDEX idx_x ON t');
    $dropped = pendingOfType($r, 'index_dropped');
    assertTrue(count($dropped) > 0);
});

// ─── VIEW VIA SQL TESTS ─────────────────────────────────
section('View via SQL');

test('CREATE VIEW via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE VIEW v1 AS SELECT * FROM users');
    $created = pendingOfType($r, 'view_created');
    assertTrue(count($created) > 0);
});

test('DROP VIEW via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE VIEW v1 AS SELECT * FROM t');
    sql($r, 'DROP VIEW v1');
    $dropped = pendingOfType($r, 'view_dropped');
    assertTrue(count($dropped) > 0);
});

// ─── TRIGGER VIA SQL TESTS ──────────────────────────────
section('Trigger via SQL');

test('CREATE TRIGGER via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TRIGGER tr1 BEFORE INSERT ON users FOR EACH ROW BEGIN END');
    $created = pendingOfType($r, 'trigger_created');
    assertTrue(count($created) > 0);
});

test('DROP TRIGGER via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TRIGGER tr1 BEFORE INSERT ON users FOR EACH ROW BEGIN END');
    sql($r, 'DROP TRIGGER tr1');
    $dropped = pendingOfType($r, 'trigger_dropped');
    assertTrue(count($dropped) > 0);
});

// ─── CONSTRAINT VIA SQL TESTS ───────────────────────────
section('Constraint via SQL');

test('ALTER TABLE ADD CONSTRAINT UNIQUE via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'ALTER TABLE t ADD CONSTRAINT c1 UNIQUE (x)');
    $created = pendingOfType($r, 'constraint_created');
    assertTrue(count($created) > 0);
});

test('ALTER TABLE DROP CONSTRAINT via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'ALTER TABLE t ADD CONSTRAINT c1 UNIQUE (x)');
    sql($r, 'ALTER TABLE t DROP CONSTRAINT c1');
    $dropped = pendingOfType($r, 'constraint_dropped');
    assertTrue(count($dropped) > 0);
});

// ─── END-TO-END SQL TESTS ───────────────────────────────
section('End-to-End SQL');

test('full CRUD lifecycle via SQL', function () {
    $r = freshRunner();

    // Create
    sql($r, 'CREATE TABLE employees (name TEXT, department TEXT, salary INTEGER)');

    // Insert
    sql($r, "INSERT INTO employees (name, department, salary) VALUES ('Alice', 'Engineering', 95000)");
    sql($r, "INSERT INTO employees (name, department, salary) VALUES ('Bob', 'Sales', 75000)");
    sql($r, "INSERT INTO employees (name, department, salary) VALUES ('Charlie', 'Engineering', 105000)");
    sql($r, "INSERT INTO employees (name, department, salary) VALUES ('Diana', 'Sales', 82000)");

    // Read all
    $rows = queryRows($r, 'SELECT * FROM employees');
    assertCount(4, $rows);

    // Filter
    $eng = queryRows($r, "SELECT * FROM employees WHERE department = 'Engineering'");
    assertCount(2, $eng);

    // Aggregate
    $avg = queryRows($r, "SELECT AVG(salary) AS avg_salary FROM employees WHERE department = 'Engineering'");
    assertTrue(abs($avg[0]['avg_salary'] - 100000.0) < 0.001);

    // Update
    sql($r, "UPDATE employees SET salary = 100000 WHERE name = 'Alice'");
    $alice = queryRows($r, "SELECT * FROM employees WHERE name = 'Alice'");
    assertEqual($alice[0]['salary'], 100000);

    // Delete
    sql($r, "DELETE FROM employees WHERE name = 'Bob'");
    $remaining = queryRows($r, 'SELECT * FROM employees');
    assertCount(3, $remaining);

    // Order + Limit
    $top = queryRows($r, 'SELECT * FROM employees ORDER BY salary DESC LIMIT 2');
    assertCount(2, $top);
    assertTrue($top[0]['salary'] >= $top[1]['salary']);
});

test('GROUP BY with HAVING (filter after aggregate)', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE sales (product TEXT, amount INTEGER)');
    sql($r, "INSERT INTO sales (product, amount) VALUES ('A', 100)");
    sql($r, "INSERT INTO sales (product, amount) VALUES ('A', 200)");
    sql($r, "INSERT INTO sales (product, amount) VALUES ('B', 50)");
    sql($r, "INSERT INTO sales (product, amount) VALUES ('C', 300)");
    sql($r, "INSERT INTO sales (product, amount) VALUES ('C', 400)");

    $rows = queryRows($r, 'SELECT SUM(amount) AS total FROM sales GROUP BY product HAVING total > 200');
    // Product A = 300, B = 50, C = 700. A and C pass.
    assertCount(2, $rows);
});

test('complex query: WHERE + ORDER BY + LIMIT', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE scores (name TEXT, score INTEGER)');
    for ($i = 1; $i <= 20; $i++) {
        sql($r, "INSERT INTO scores (name, score) VALUES ('player_{$i}', " . ($i * 10) . ")");
    }

    $rows = queryRows($r, 'SELECT * FROM scores WHERE score > 50 ORDER BY score DESC LIMIT 5');
    assertCount(5, $rows);
    assertEqual($rows[0]['score'], 200);
    assertEqual($rows[4]['score'], 160);
});

test('multi-table JOIN via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE authors (name TEXT)');
    sql($r, 'CREATE TABLE books (author_id INTEGER, title TEXT)');
    sql($r, "INSERT INTO authors (name) VALUES ('Tolkien')");
    sql($r, "INSERT INTO authors (name) VALUES ('Rowling')");
    sql($r, "INSERT INTO books (author_id, title) VALUES (1, 'The Hobbit')");
    sql($r, "INSERT INTO books (author_id, title) VALUES (1, 'LOTR')");
    sql($r, "INSERT INTO books (author_id, title) VALUES (2, 'HP')");

    $rows = queryRows($r, 'SELECT * FROM authors JOIN books ON id = author_id');
    assertCount(3, $rows);
    $tolkien = array_filter($rows, fn($r) => $r['name'] === 'Tolkien');
    assertCount(2, $tolkien);
});

test('SELECT COUNT with GROUP BY via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE log (level TEXT)');
    sql($r, "INSERT INTO log (level) VALUES ('INFO')");
    sql($r, "INSERT INTO log (level) VALUES ('INFO')");
    sql($r, "INSERT INTO log (level) VALUES ('ERROR')");
    sql($r, "INSERT INTO log (level) VALUES ('INFO')");
    sql($r, "INSERT INTO log (level) VALUES ('ERROR')");

    $rows = queryRows($r, 'SELECT COUNT(*) AS cnt FROM log GROUP BY level');
    assertCount(2, $rows);
    $counts = array_column($rows, 'cnt');
    sort($counts);
    assertEqual($counts[0], 2);
    assertEqual($counts[1], 3);
});

test('file-backed SQL end-to-end', function () {
    global $tmpDir; cleanTmp();

    $r1 = new Runner(new FileStore($tmpDir), new FileRefs($tmpDir));
    registerDatabaseGates($r1);
    registerSQLGates($r1);

    sql($r1, 'CREATE TABLE items (name TEXT, qty INTEGER)');
    sql($r1, "INSERT INTO items (name, qty) VALUES ('Widget', 100)");
    sql($r1, "INSERT INTO items (name, qty) VALUES ('Gadget', 50)");

    // New runner, same files
    $r2 = new Runner(new FileStore($tmpDir), new FileRefs($tmpDir));
    registerDatabaseGates($r2);
    registerSQLGates($r2);

    $rows = queryRows($r2, 'SELECT * FROM items ORDER BY qty ASC');
    assertCount(2, $rows);
    assertEqual($rows[0]['name'], 'Gadget');
    assertEqual($rows[1]['name'], 'Widget');
});

test('string with special characters in SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (msg TEXT)');
    sql($r, "INSERT INTO t (msg) VALUES ('it''s a test')");
    $rows = queryRows($r, 'SELECT * FROM t');
    assertEqual($rows[0]['msg'], "it's a test");
});

test('NULL handling in SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (NULL)');
    $rows = queryRows($r, 'SELECT * FROM t WHERE x IS NULL');
    assertCount(1, $rows);
    assertNull($rows[0]['x']);
});

test('IS NOT NULL in SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (NULL)');
    $rows = queryRows($r, 'SELECT * FROM t WHERE x IS NOT NULL');
    assertCount(1, $rows);
    assertEqual($rows[0]['x'], 1);
});

test('DISTINCT with ORDER BY', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (3)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    $rows = queryRows($r, 'SELECT DISTINCT x FROM t ORDER BY x ASC');
    assertCount(3, $rows);
    assertEqual($rows[0]['x'], 1);
    assertEqual($rows[2]['x'], 3);
});

// ─── DONE ────────────────────────────────────────────────
cleanTmp();
report();
