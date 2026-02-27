<?php
/**
 * ICE Database — Phase 12 Test Suite
 * SQL Completeness: UPDATE expressions, INSERT...SELECT, CTAS, IF NOT EXISTS, EXPLAIN.
 */
require __DIR__ . '/runner.php';
require __DIR__ . '/../ice/autoload.php';

use Ice\Core\Event;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Resolution\Runner;

use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;

echo "ICE Database — Phase 12: SQL Completeness\n";

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

function lastEvent(Runner $r, string $type): ?Event {
    $events = array_values(array_filter($r->sample()['pending'], fn($e) => $e->type === $type));
    return count($events) > 0 ? end($events) : null;
}

function hasError(Runner $r): bool {
    return lastEvent($r, 'error') !== null;
}

function setupProducts(Runner $r): void {
    sql($r, 'CREATE TABLE products (name TEXT, price INTEGER, qty INTEGER)');
    sql($r, "INSERT INTO products (name, price, qty) VALUES ('Widget', 10, 5)");
    sql($r, "INSERT INTO products (name, price, qty) VALUES ('Gadget', 25, 3)");
    sql($r, "INSERT INTO products (name, price, qty) VALUES ('Doohickey', 7, 12)");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// UPDATE WITH EXPRESSIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('UPDATE with expressions');

test('UPDATE SET col = col + literal', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'UPDATE products SET price = price + 5 WHERE name = \'Widget\'');
    $rows = queryRows($r, "SELECT price FROM products WHERE name = 'Widget'");
    assertEqual($rows[0]['price'], 15);
});

test('UPDATE SET col = col * literal', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'UPDATE products SET price = price * 2');
    $rows = queryRows($r, 'SELECT name, price FROM products ORDER BY name');
    assertEqual($rows[0]['price'], 14);  // Doohickey 7*2
    assertEqual($rows[1]['price'], 50);  // Gadget 25*2
    assertEqual($rows[2]['price'], 20);  // Widget 10*2
});

test('UPDATE SET col = expression referencing other cols', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'UPDATE products SET qty = price + qty WHERE name = \'Widget\'');
    $rows = queryRows($r, "SELECT qty FROM products WHERE name = 'Widget'");
    assertEqual($rows[0]['qty'], 15); // 10+5
});

test('UPDATE SET with function', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, "UPDATE products SET name = UPPER(name) WHERE name = 'Widget'");
    $rows = queryRows($r, "SELECT name FROM products WHERE name = 'WIDGET'");
    assertCount(1, $rows);
});

test('UPDATE mixed literal and expression SET', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, "UPDATE products SET price = price + 1, name = 'Updated' WHERE name = 'Widget'");
    $rows = queryRows($r, "SELECT name, price FROM products WHERE name = 'Updated'");
    assertCount(1, $rows);
    assertEqual($rows[0]['price'], 11);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// IF NOT EXISTS / IF EXISTS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('IF NOT EXISTS / IF EXISTS');

test('CREATE TABLE IF NOT EXISTS on new table', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE IF NOT EXISTS t (x INTEGER)');
    $e = lastEvent($r, 'table_created');
    assertTrue($e !== null);
});

test('CREATE TABLE IF NOT EXISTS on existing table', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    $r->clearPending();
    sql($r, 'CREATE TABLE IF NOT EXISTS t (x INTEGER)');
    assertTrue(!hasError($r));
    $e = lastEvent($r, 'table_exists');
    assertTrue($e !== null);
});

test('CREATE TABLE without IF NOT EXISTS errors on duplicate', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    $r->clearPending();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    assertTrue(hasError($r));
});

test('DROP TABLE IF EXISTS on missing table', function () {
    $r = freshRunner();
    $r->clearPending();
    sql($r, 'DROP TABLE IF EXISTS nonexistent');
    assertTrue(!hasError($r));
});

test('DROP TABLE IF EXISTS on existing table', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    $r->clearPending();
    sql($r, 'DROP TABLE IF EXISTS t');
    assertTrue(!hasError($r));
    $e = lastEvent($r, 'table_dropped');
    assertTrue($e !== null);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// INSERT...SELECT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('INSERT...SELECT');

test('INSERT INTO t SELECT * FROM source', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'CREATE TABLE archive (name TEXT, price INTEGER, qty INTEGER)');
    sql($r, 'INSERT INTO archive SELECT name, price, qty FROM products');
    $rows = queryRows($r, 'SELECT * FROM archive');
    assertCount(3, $rows);
});

test('INSERT...SELECT with WHERE', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'CREATE TABLE expensive (name TEXT, price INTEGER)');
    sql($r, 'INSERT INTO expensive (name, price) SELECT name, price FROM products WHERE price > 8');
    $rows = queryRows($r, 'SELECT * FROM expensive ORDER BY price');
    assertCount(2, $rows);
    assertEqual($rows[0]['name'], 'Widget');
    assertEqual($rows[1]['name'], 'Gadget');
});

test('INSERT...SELECT with expression', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'CREATE TABLE revenue (name TEXT, rev INTEGER)');
    sql($r, 'INSERT INTO revenue (name, rev) SELECT name, price * qty FROM products');
    $rows = queryRows($r, 'SELECT * FROM revenue ORDER BY rev DESC');
    assertCount(3, $rows);
    assertEqual($rows[0]['rev'], 84); // Doohickey 7*12
});

test('INSERT...SELECT assigns new IDs', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'CREATE TABLE copy (name TEXT)');
    sql($r, 'INSERT INTO copy (name) SELECT name FROM products');
    $rows = queryRows($r, 'SELECT id, name FROM copy ORDER BY id');
    assertEqual($rows[0]['id'], 1);
    assertEqual($rows[1]['id'], 2);
    assertEqual($rows[2]['id'], 3);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CREATE TABLE AS SELECT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('CREATE TABLE AS SELECT');

test('CTAS basic', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'CREATE TABLE backup AS SELECT name, price FROM products');
    $rows = queryRows($r, 'SELECT * FROM backup ORDER BY name');
    assertCount(3, $rows);
    assertEqual($rows[0]['name'], 'Doohickey');
    assertEqual($rows[0]['price'], 7);
});

test('CTAS with WHERE', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'CREATE TABLE cheap AS SELECT name, price FROM products WHERE price < 10');
    $rows = queryRows($r, 'SELECT * FROM cheap');
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Doohickey');
});

test('CTAS with expression', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'CREATE TABLE summary AS SELECT name, price * qty AS revenue FROM products');
    $rows = queryRows($r, 'SELECT * FROM summary ORDER BY revenue DESC');
    assertCount(3, $rows);
    assertEqual($rows[0]['revenue'], 84);
});

test('CTAS IF NOT EXISTS on existing table', function () {
    $r = freshRunner();
    setupProducts($r);
    $r->clearPending();
    sql($r, 'CREATE TABLE IF NOT EXISTS products AS SELECT name FROM products');
    assertTrue(!hasError($r));
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// EXPLAIN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('EXPLAIN');

test('EXPLAIN simple SELECT', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, 'EXPLAIN SELECT * FROM products');
    assertTrue(count($rows) >= 1);
    assertEqual($rows[0]['operation'], 'SCAN products');
});

test('EXPLAIN SELECT with WHERE', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, 'EXPLAIN SELECT name FROM products WHERE price > 10');
    $ops = array_column($rows, 'operation');
    assertTrue(in_array('SCAN products', $ops));
    assertTrue(in_array('FILTER', $ops));
});

test('EXPLAIN SELECT with JOIN', function () {
    $r = freshRunner();
    setupProducts($r);
    sql($r, 'CREATE TABLE orders (product TEXT, amount INTEGER)');
    $rows = queryRows($r, 'EXPLAIN SELECT * FROM products JOIN orders ON name = product');
    $ops = array_column($rows, 'operation');
    assertTrue(in_array('SCAN products', $ops));
    $hasJoin = false;
    foreach ($ops as $op) { if (str_contains($op, 'JOIN')) $hasJoin = true; }
    assertTrue($hasJoin);
});

test('EXPLAIN SELECT with ORDER BY and LIMIT', function () {
    $r = freshRunner();
    setupProducts($r);
    $rows = queryRows($r, 'EXPLAIN SELECT name FROM products ORDER BY price LIMIT 2');
    $ops = array_column($rows, 'operation');
    assertTrue(in_array('ORDER BY', $ops));
    $hasLimit = false;
    foreach ($ops as $op) { if (str_starts_with($op, 'LIMIT')) $hasLimit = true; }
    assertTrue($hasLimit);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// END-TO-END INTEGRATION
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('End-to-End Integration');

test('UPDATE + INSERT...SELECT + query', function () {
    $r = freshRunner();
    setupProducts($r);
    // Double price for items with qty <= 5 (Widget qty=5, Gadget qty=3)
    sql($r, 'UPDATE products SET price = price * 2 WHERE qty <= 5');
    // Widget: 10*2=20, Gadget: 25*2=50, Doohickey: 7 (unchanged)
    sql($r, 'CREATE TABLE report AS SELECT name, price FROM products WHERE price > 15');
    $rows = queryRows($r, 'SELECT * FROM report ORDER BY price DESC');
    assertCount(2, $rows);
    assertEqual($rows[0]['name'], 'Gadget');  // 50
    assertEqual($rows[1]['name'], 'Widget');   // 20
});

test('CTAS + INSERT...SELECT chained', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE source (x INTEGER)');
    sql($r, 'INSERT INTO source (x) VALUES (1), (2), (3)');
    sql($r, 'CREATE TABLE doubled AS SELECT x * 2 AS val FROM source');
    sql($r, 'CREATE TABLE tripled (val INTEGER)');
    sql($r, 'INSERT INTO tripled (val) SELECT val FROM doubled');
    $rows = queryRows($r, 'SELECT val FROM tripled ORDER BY val');
    assertEqual($rows[0]['val'], 2);
    assertEqual($rows[1]['val'], 4);
    assertEqual($rows[2]['val'], 6);
});

report();
