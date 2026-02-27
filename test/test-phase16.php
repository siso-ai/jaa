<?php
/**
 * ICE Database — Phase 16 Test Suite
 * ILIKE, NOT BETWEEN, expanded string/math/utility functions.
 */
require __DIR__ . '/runner.php';
require __DIR__ . '/../ice/autoload.php';

use Ice\Core\Event;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Resolution\Runner;

use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;

echo "ICE Database — Phase 16: Operators & Functions Expansion\n";

function freshRunner(): Runner {
    $r = new Runner(new MemoryStore(), new MemoryRefs());
    registerDatabaseGates($r);
    registerSQLGates($r);
    return $r;
}

function sql(Runner $r, string $query): void {
    $r->emit(new Event('sql', ['sql' => $query]));
}

function queryRows(Runner $r, string $query): array {
    $r->clearPending();
    sql($r, $query);
    $results = array_values(array_filter($r->sample()['pending'], fn($e) => $e->type === 'query_result'));
    return count($results) > 0 ? end($results)->data['rows'] : [];
}

function setupItems(Runner $r): void {
    sql($r, 'CREATE TABLE items (name TEXT, category TEXT, price INTEGER)');
    sql($r, "INSERT INTO items (name, category, price) VALUES ('Widget', 'Tools', 25)");
    sql($r, "INSERT INTO items (name, category, price) VALUES ('Gadget', 'Electronics', 50)");
    sql($r, "INSERT INTO items (name, category, price) VALUES ('gizmo', 'electronics', 15)");
    sql($r, "INSERT INTO items (name, category, price) VALUES ('Bolt', 'Hardware', 3)");
    sql($r, "INSERT INTO items (name, category, price) VALUES ('Nut', 'Hardware', 2)");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('LIKE — case-sensitive');

test('LIKE is case-sensitive', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT name FROM items WHERE name LIKE 'G%'");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Gadget');
});

test('LIKE with underscore', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT name FROM items WHERE name LIKE '_ut'");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Nut');
});

test('NOT LIKE', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT name FROM items WHERE name NOT LIKE '%et'");
    // Widget, Gadget end with 'et'; gizmo, Bolt, Nut don't
    assertCount(3, $rows);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('ILIKE — case-insensitive');

test('ILIKE matches case-insensitively', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT name FROM items WHERE name ILIKE 'g%'");
    assertCount(2, $rows); // Gadget and gizmo
});

test('ILIKE with category', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT name FROM items WHERE category ILIKE 'electronics'");
    assertCount(2, $rows); // Electronics and electronics
});

test('NOT ILIKE', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT name FROM items WHERE name NOT ILIKE 'g%'");
    assertCount(3, $rows); // Widget, Bolt, Nut
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('NOT BETWEEN');

test('NOT BETWEEN excludes range', function () {
    $r = freshRunner(); setupItems($r);
    // Prices: Widget=25, Gadget=50, gizmo=15, Bolt=3, Nut=2
    // NOT BETWEEN 10 AND 30 → Gadget=50, Bolt=3, Nut=2
    $rows = queryRows($r, "SELECT name FROM items WHERE price NOT BETWEEN 10 AND 30 ORDER BY name");
    assertCount(3, $rows);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('String functions — LEFT, RIGHT, REVERSE, REPEAT');

test('LEFT(str, n)', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT LEFT(name, 3) AS prefix FROM items WHERE name = 'Widget'");
    assertEqual($rows[0]['prefix'], 'Wid');
});

test('RIGHT(str, n)', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT RIGHT(name, 3) AS suffix FROM items WHERE name = 'Widget'");
    assertEqual($rows[0]['suffix'], 'get');
});

test('REVERSE(str)', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT REVERSE(name) AS rev FROM items WHERE name = 'Bolt'");
    assertEqual($rows[0]['rev'], 'tloB');
});

test('REPEAT(str, n)', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT REPEAT(name, 2) AS doubled FROM items WHERE name = 'Nut'");
    assertEqual($rows[0]['doubled'], 'NutNut');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('String functions — LPAD, RPAD, POSITION');

test('LPAD(str, len, fill)', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT LPAD(name, 8, '*') AS padded FROM items WHERE name = 'Bolt'");
    assertEqual($rows[0]['padded'], '****Bolt');
});

test('RPAD(str, len, fill)', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT RPAD(name, 8, '.') AS padded FROM items WHERE name = 'Bolt'");
    assertEqual($rows[0]['padded'], 'Bolt....');
});

test('POSITION(str, substr)', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT POSITION(name, 'dg') AS pos FROM items WHERE name = 'Widget'");
    assertEqual($rows[0]['pos'], 3);
});

test('POSITION returns 0 when not found', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT POSITION(name, 'xyz') AS pos FROM items WHERE name = 'Widget'");
    assertEqual($rows[0]['pos'], 0);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('String functions — CHAR_LENGTH, STARTS_WITH, ENDS_WITH');

test('CHAR_LENGTH', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT CHAR_LENGTH(name) AS len FROM items WHERE name = 'Widget'");
    assertEqual($rows[0]['len'], 6);
});

test('STARTS_WITH', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT name FROM items WHERE STARTS_WITH(name, 'Ga') = 1");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Gadget');
});

test('ENDS_WITH', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT name FROM items WHERE ENDS_WITH(name, 'et') = 1");
    assertCount(2, $rows); // Widget, Gadget
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Math functions — CEIL, FLOOR, POWER, SQRT');

test('CEIL and FLOOR', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE nums (val REAL)');
    sql($r, 'INSERT INTO nums (val) VALUES (3.7)');
    $rows = queryRows($r, 'SELECT CEIL(val) AS c, FLOOR(val) AS f FROM nums');
    assertEqual($rows[0]['c'], 4);
    assertEqual($rows[0]['f'], 3);
});

test('POWER and SQRT', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE nums (val INTEGER)');
    sql($r, 'INSERT INTO nums (val) VALUES (9)');
    $rows = queryRows($r, 'SELECT POWER(val, 2) AS sq, SQRT(val) AS rt FROM nums');
    assertEqual($rows[0]['sq'], 81);
    assertEqual($rows[0]['rt'], 3.0);
});

test('MOD', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE nums (val INTEGER)');
    sql($r, 'INSERT INTO nums (val) VALUES (17)');
    $rows = queryRows($r, 'SELECT MOD(val, 5) AS m FROM nums');
    assertEqual($rows[0]['m'], 2.0);
});

test('SIGN', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE nums (val INTEGER)');
    sql($r, 'INSERT INTO nums (val) VALUES (-5)');
    sql($r, 'INSERT INTO nums (val) VALUES (0)');
    sql($r, 'INSERT INTO nums (val) VALUES (7)');
    $rows = queryRows($r, 'SELECT val, SIGN(val) AS s FROM nums ORDER BY val');
    assertEqual($rows[0]['s'], -1);
    assertEqual($rows[1]['s'], 0);
    assertEqual($rows[2]['s'], 1);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Math functions — LOG, LN, EXP, PI');

test('LN and EXP', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE nums (val REAL)');
    sql($r, 'INSERT INTO nums (val) VALUES (1)');
    $rows = queryRows($r, 'SELECT LN(val) AS l, EXP(val) AS e FROM nums');
    assertEqual($rows[0]['l'], 0.0);
    assertTrue(abs($rows[0]['e'] - 2.718) < 0.01);
});

test('PI()', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE nums (val INTEGER)');
    sql($r, 'INSERT INTO nums (val) VALUES (1)');
    $rows = queryRows($r, 'SELECT PI() AS pi FROM nums');
    assertTrue(abs($rows[0]['pi'] - 3.14159) < 0.001);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Utility functions — TYPEOF, GREATEST, LEAST');

test('TYPEOF', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT TYPEOF(name) AS tn, TYPEOF(price) AS tp FROM items WHERE name = 'Widget'");
    assertEqual($rows[0]['tn'], 'text');
    assertEqual($rows[0]['tp'], 'integer');
});

test('GREATEST', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE nums (a INTEGER, b INTEGER, c INTEGER)');
    sql($r, 'INSERT INTO nums (a, b, c) VALUES (10, 30, 20)');
    $rows = queryRows($r, 'SELECT GREATEST(a, b, c) AS mx FROM nums');
    assertEqual($rows[0]['mx'], 30);
});

test('LEAST', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE nums (a INTEGER, b INTEGER, c INTEGER)');
    sql($r, 'INSERT INTO nums (a, b, c) VALUES (10, 30, 20)');
    $rows = queryRows($r, 'SELECT LEAST(a, b, c) AS mn FROM nums');
    assertEqual($rows[0]['mn'], 10);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Integration');

test('ILIKE + string function in SELECT', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, "SELECT UPPER(name) AS n, LPAD(CAST(price AS TEXT), 5, '0') AS p FROM items WHERE category ILIKE 'hardware' ORDER BY name");
    assertCount(2, $rows);
    assertEqual($rows[0]['n'], 'BOLT');
    assertEqual($rows[0]['p'], '00003');
});

test('BETWEEN + CEIL in expression', function () {
    $r = freshRunner(); setupItems($r);
    $rows = queryRows($r, 'SELECT name, CEIL(price * 1.1) AS tax_price FROM items WHERE price BETWEEN 10 AND 30 ORDER BY name');
    assertCount(2, $rows); // Widget=25, gizmo=15
});

test('NOT BETWEEN + GREATEST', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE scores (name TEXT, math INTEGER, english INTEGER, science INTEGER)');
    sql($r, "INSERT INTO scores (name, math, english, science) VALUES ('Alice', 85, 92, 78)");
    sql($r, "INSERT INTO scores (name, math, english, science) VALUES ('Bob', 60, 55, 70)");
    $rows = queryRows($r, "SELECT name, GREATEST(math, english, science) AS best FROM scores WHERE GREATEST(math, english, science) NOT BETWEEN 70 AND 80");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Alice');
    assertEqual($rows[0]['best'], 92);
});

report();
