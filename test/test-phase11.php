<?php
/**
 * ICE Database — Phase 11 Test Suite
 * SQL Expression Engine: arithmetic, functions, CASE WHEN, UNION.
 */
require __DIR__ . '/runner.php';
require __DIR__ . '/../ice/autoload.php';

use Ice\Core\Event;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Resolution\Runner;

use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;

echo "ICE Database — Phase 11: SQL Expression Engine\n";

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
    if (count($results) === 0) return [];
    return end($results)->data['rows'];
}

function setupProducts(Runner $r): void {
    sql($r, 'CREATE TABLE products (name TEXT, price INTEGER, qty INTEGER)');
    sql($r, "INSERT INTO products (name, price, qty) VALUES ('Widget', 10, 5)");
    sql($r, "INSERT INTO products (name, price, qty) VALUES ('Gadget', 25, 3)");
    sql($r, "INSERT INTO products (name, price, qty) VALUES ('Doohickey', 7, 12)");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ARITHMETIC EXPRESSIONS IN SELECT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Arithmetic expressions in SELECT');

test('SELECT col * col AS alias', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, 'SELECT name, price * qty AS total FROM products ORDER BY total DESC');
    assertCount(3, $rows);
    assertEqual($rows[0]['total'], 84); // Doohickey 7*12
    assertEqual($rows[1]['total'], 75); // Gadget 25*3
    assertEqual($rows[2]['total'], 50); // Widget 10*5
});

test('SELECT col + col', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, 'SELECT name, price + qty AS total_sum FROM products WHERE name = \'Widget\'');
    assertEqual($rows[0]['total_sum'], 15);
});

test('SELECT col - literal', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, 'SELECT name, price - 5 AS discounted FROM products WHERE name = \'Widget\'');
    assertEqual($rows[0]['discounted'], 5);
});

test('SELECT col / literal', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, 'SELECT name, price / 2 AS half_price FROM products WHERE name = \'Widget\'');
    assertEqual($rows[0]['half_price'], 5);
});

test('SELECT parenthesized expression', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, 'SELECT name, (price + 5) * qty AS boosted FROM products WHERE name = \'Widget\'');
    assertEqual($rows[0]['boosted'], 75); // (10+5)*5
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// EXPRESSIONS IN WHERE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Expressions in WHERE');

test('WHERE expr > literal', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, 'SELECT name FROM products WHERE price * qty > 60');
    assertCount(2, $rows);
});

test('WHERE expr = expr', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (a INTEGER, b INTEGER)');
    sql($r, 'INSERT INTO t (a, b) VALUES (5, 5)');
    sql($r, 'INSERT INTO t (a, b) VALUES (3, 7)');
    $rows = queryRows($r, 'SELECT * FROM t WHERE a + b = 10');
    assertCount(2, $rows);
});

test('WHERE col compared to col', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, 'SELECT name FROM products WHERE price > qty');
    assertCount(2, $rows); // Widget (10>5) and Gadget (25>3)
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SQL BUILT-IN FUNCTIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('SQL Functions');

test('UPPER()', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT UPPER(name) AS upper_name FROM products WHERE name = 'Widget'");
    assertEqual($rows[0]['upper_name'], 'WIDGET');
});

test('LOWER()', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT LOWER(name) AS lower_name FROM products WHERE name = 'Widget'");
    assertEqual($rows[0]['lower_name'], 'widget');
});

test('LENGTH()', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT LENGTH(name) AS len FROM products WHERE name = 'Widget'");
    assertEqual($rows[0]['len'], 6);
});

test('ABS()', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (-42)');
    $rows = queryRows($r, 'SELECT ABS(x) AS abs_x FROM t');
    assertEqual($rows[0]['abs_x'], 42);
});

test('ROUND()', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x REAL)');
    sql($r, 'INSERT INTO t (x) VALUES (3.14159)');
    $rows = queryRows($r, 'SELECT ROUND(x, 2) AS rounded FROM t');
    assertTrue(abs($rows[0]['rounded'] - 3.14) < 0.001);
});

test('CONCAT()', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT CONCAT(name, ' costs ', price) AS label FROM products WHERE name = 'Widget'");
    assertEqual($rows[0]['label'], 'Widget costs 10');
});

test('|| string concatenation', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT name || '-' || price AS label FROM products WHERE name = 'Gadget'");
    assertEqual($rows[0]['label'], 'Gadget-25');
});

test('SUBSTR()', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT SUBSTR(name, 1, 3) AS short FROM products WHERE name = 'Widget'");
    assertEqual($rows[0]['short'], 'Wid');
});

test('REPLACE()', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT REPLACE(name, 'get', 'gizmo') AS replaced FROM products WHERE name = 'Widget'");
    assertEqual($rows[0]['replaced'], 'Widgizmo');
});

test('TRIM()', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (s TEXT)');
    sql($r, "INSERT INTO t (s) VALUES ('  hello  ')");
    $rows = queryRows($r, 'SELECT TRIM(s) AS trimmed FROM t');
    assertEqual($rows[0]['trimmed'], 'hello');
});

test('COALESCE()', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (a INTEGER, b INTEGER)');
    sql($r, 'INSERT INTO t (b) VALUES (42)');
    $rows = queryRows($r, 'SELECT COALESCE(a, b, 0) AS result FROM t');
    assertEqual($rows[0]['result'], 42);
});

test('IFNULL()', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (a INTEGER, b INTEGER)');
    sql($r, 'INSERT INTO t (b) VALUES (99)');
    $rows = queryRows($r, 'SELECT IFNULL(a, b) AS result FROM t');
    assertEqual($rows[0]['result'], 99);
});

test('NULLIF()', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (a INTEGER)');
    sql($r, 'INSERT INTO t (a) VALUES (5)');
    sql($r, 'INSERT INTO t (a) VALUES (0)');
    $rows = queryRows($r, 'SELECT a, NULLIF(a, 0) AS result FROM t ORDER BY a');
    assertNull($rows[0]['result']); // 0 NULLIF 0 = null
    assertEqual($rows[1]['result'], 5);
});

test('CAST()', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x TEXT)');
    sql($r, "INSERT INTO t (x) VALUES ('42')");
    $rows = queryRows($r, 'SELECT CAST(x AS INTEGER) AS num FROM t');
    assertTrue($rows[0]['num'] === 42);
});

test('Nested functions', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT UPPER(SUBSTR(name, 1, 3)) AS code FROM products WHERE name = 'Widget'");
    assertEqual($rows[0]['code'], 'WID');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CASE WHEN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('CASE WHEN');

test('CASE WHEN basic', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT name, price, CASE WHEN price > 20 THEN 'expensive' WHEN price > 8 THEN 'medium' ELSE 'cheap' END AS tier FROM products ORDER BY price DESC");
    assertEqual($rows[0]['tier'], 'expensive'); // Gadget 25
    assertEqual($rows[1]['tier'], 'medium');    // Widget 10
    assertEqual($rows[2]['tier'], 'cheap');     // Doohickey 7
});

test('CASE WHEN without ELSE', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT name, CASE WHEN price > 20 THEN 'expensive' END AS tier FROM products WHERE name = 'Widget'");
    assertNull($rows[0]['tier']); // price 10, no matching WHEN, no ELSE
});

test('CASE WHEN with expression', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT name, CASE WHEN price * qty > 60 THEN 'high' ELSE 'low' END AS volume FROM products ORDER BY name");
    assertEqual($rows[0]['volume'], 'high'); // Doohickey 7*12=84
    assertEqual($rows[1]['volume'], 'high'); // Gadget 25*3=75
    assertEqual($rows[2]['volume'], 'low');  // Widget 10*5=50
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// UNION / UNION ALL
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('UNION');

test('UNION ALL combines rows', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE a (x INTEGER)');
    sql($r, 'CREATE TABLE b (x INTEGER)');
    sql($r, 'INSERT INTO a (x) VALUES (1), (2)');
    sql($r, 'INSERT INTO b (x) VALUES (3), (4)');
    $rows = queryRows($r, 'SELECT x FROM a UNION ALL SELECT x FROM b');
    assertCount(4, $rows);
});

test('UNION removes duplicates', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE a (x INTEGER)');
    sql($r, 'CREATE TABLE b (x INTEGER)');
    sql($r, 'INSERT INTO a (x) VALUES (1), (2), (3)');
    sql($r, 'INSERT INTO b (x) VALUES (2), (3), (4)');
    $rows = queryRows($r, 'SELECT x FROM a UNION SELECT x FROM b');
    assertCount(4, $rows); // 1,2,3,4 deduplicated
});

test('UNION ALL keeps duplicates', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE a (x INTEGER)');
    sql($r, 'CREATE TABLE b (x INTEGER)');
    sql($r, 'INSERT INTO a (x) VALUES (1), (2)');
    sql($r, 'INSERT INTO b (x) VALUES (2), (3)');
    $rows = queryRows($r, 'SELECT x FROM a UNION ALL SELECT x FROM b');
    assertCount(4, $rows); // 1,2,2,3 — not deduplicated
});

test('UNION with WHERE on both sides', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE a (x INTEGER, label TEXT)');
    sql($r, 'CREATE TABLE b (x INTEGER, label TEXT)');
    sql($r, "INSERT INTO a (x, label) VALUES (1, 'a'), (2, 'a'), (3, 'a')");
    sql($r, "INSERT INTO b (x, label) VALUES (4, 'b'), (5, 'b'), (6, 'b')");
    $rows = queryRows($r, "SELECT x FROM a WHERE x > 1 UNION ALL SELECT x FROM b WHERE x < 6");
    assertCount(4, $rows); // 2,3,4,5
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// END-TO-END INTEGRATION
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('End-to-End Integration');

test('complex expression query', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "
        SELECT
            UPPER(name) AS product,
            price * qty AS revenue,
            CASE WHEN price * qty > 60 THEN 'A' ELSE 'B' END AS grade
        FROM products
        WHERE price * qty > 40
        ORDER BY revenue DESC
    ");
    assertCount(3, $rows);
    assertEqual($rows[0]['product'], 'DOOHICKEY');
    assertEqual($rows[0]['revenue'], 84);
    assertEqual($rows[0]['grade'], 'A');
});

test('function in WHERE and SELECT', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "SELECT LOWER(name) AS n FROM products WHERE LENGTH(name) > 6");
    assertCount(1, $rows);
    assertEqual($rows[0]['n'], 'doohickey');
});

test('UNION with expressions', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'CREATE TABLE deals (item TEXT, discount INTEGER)');
    sql($r, "INSERT INTO deals (item, discount) VALUES ('Widget', 2), ('Gadget', 5)");
    $rows = queryRows($r, "
        SELECT name AS item, price AS amount FROM products WHERE price > 8
        UNION ALL
        SELECT item, discount AS amount FROM deals
    ");
    assertCount(4, $rows); // 2 products + 2 deals
});

test('multiple expressions in single SELECT', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, "
        SELECT
            name,
            price * qty AS revenue,
            price + qty AS combined,
            price - qty AS diff
        FROM products
        WHERE name = 'Widget'
    ");
    assertEqual($rows[0]['revenue'], 50);
    assertEqual($rows[0]['combined'], 15);
    assertEqual($rows[0]['diff'], 5);
});

report();
