<?php
/**
 * ICE Database — Phase 15 Test Suite
 * UPSERT (ON CONFLICT), RETURNING, TRUNCATE TABLE, Multi-JOIN.
 */
require __DIR__ . '/runner.php';
require __DIR__ . '/../ice/autoload.php';

use Ice\Core\Event;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Resolution\Runner;

use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;

echo "ICE Database — Phase 15: UPSERT, RETURNING, TRUNCATE & Multi-JOIN\n";

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

function allEvents(Runner $r, string $query): array {
    $r->clearPending();
    sql($r, $query);
    return $r->sample()['pending'];
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// TRUNCATE TABLE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('TRUNCATE TABLE');

test('TRUNCATE TABLE removes all rows', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE items (name TEXT, price INTEGER)');
    sql($r, "INSERT INTO items (name, price) VALUES ('A', 10)");
    sql($r, "INSERT INTO items (name, price) VALUES ('B', 20)");
    sql($r, "INSERT INTO items (name, price) VALUES ('C', 30)");
    $rows = queryRows($r, 'SELECT * FROM items');
    assertCount(3, $rows);
    sql($r, 'TRUNCATE TABLE items');
    $rows = queryRows($r, 'SELECT * FROM items');
    assertCount(0, $rows);
});

test('TRUNCATE without TABLE keyword', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE items (name TEXT)');
    sql($r, "INSERT INTO items (name) VALUES ('A')");
    sql($r, 'TRUNCATE items');
    $rows = queryRows($r, 'SELECT * FROM items');
    assertCount(0, $rows);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// UPSERT — ON CONFLICT DO NOTHING
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('UPSERT — ON CONFLICT DO NOTHING');

test('INSERT ON CONFLICT (col) DO NOTHING — no conflict', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE users (email TEXT, name TEXT)');
    sql($r, "INSERT INTO users (email, name) VALUES ('a@test.com', 'Alice')");
    sql($r, "INSERT INTO users (email, name) VALUES ('b@test.com', 'Bob') ON CONFLICT (email) DO NOTHING");
    $rows = queryRows($r, 'SELECT * FROM users ORDER BY email');
    assertCount(2, $rows);
});

test('INSERT ON CONFLICT (col) DO NOTHING — with conflict', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE users (email TEXT, name TEXT)');
    sql($r, "INSERT INTO users (email, name) VALUES ('a@test.com', 'Alice')");
    sql($r, "INSERT INTO users (email, name) VALUES ('a@test.com', 'Alice2') ON CONFLICT (email) DO NOTHING");
    $rows = queryRows($r, 'SELECT * FROM users');
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Alice'); // Not updated
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// UPSERT — ON CONFLICT DO UPDATE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('UPSERT — ON CONFLICT DO UPDATE');

test('INSERT ON CONFLICT DO UPDATE SET — updates existing row', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE users (email TEXT, name TEXT, visits INTEGER)');
    sql($r, "INSERT INTO users (email, name, visits) VALUES ('a@test.com', 'Alice', 1)");
    sql($r, "INSERT INTO users (email, name, visits) VALUES ('a@test.com', 'Alice', 1) ON CONFLICT (email) DO UPDATE SET visits = visits + 1");
    $rows = queryRows($r, 'SELECT * FROM users');
    assertCount(1, $rows);
    assertEqual($rows[0]['visits'], 2);
});

test('INSERT ON CONFLICT DO UPDATE SET — multiple columns', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE users (email TEXT, name TEXT, visits INTEGER)');
    sql($r, "INSERT INTO users (email, name, visits) VALUES ('a@test.com', 'Alice', 1)");
    sql($r, "INSERT INTO users (email, name, visits) VALUES ('a@test.com', 'Alice Updated', 99) ON CONFLICT (email) DO UPDATE SET name = 'Alice V2', visits = visits + 1");
    $rows = queryRows($r, 'SELECT * FROM users');
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Alice V2');
    assertEqual($rows[0]['visits'], 2);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// RETURNING — INSERT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('RETURNING — INSERT');

test('INSERT RETURNING *', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE items (name TEXT, price INTEGER)');
    $rows = queryRows($r, "INSERT INTO items (name, price) VALUES ('Widget', 25) RETURNING *");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Widget');
    assertEqual($rows[0]['price'], 25);
    assertTrue(isset($rows[0]['id']));
});

test('INSERT RETURNING specific columns', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE items (name TEXT, price INTEGER)');
    $rows = queryRows($r, "INSERT INTO items (name, price) VALUES ('Widget', 25) RETURNING id, name");
    assertCount(1, $rows);
    assertTrue(isset($rows[0]['id']));
    assertEqual($rows[0]['name'], 'Widget');
    assertTrue(!isset($rows[0]['price']));
});

test('UPSERT + RETURNING', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE users (email TEXT, name TEXT)');
    sql($r, "INSERT INTO users (email, name) VALUES ('a@test.com', 'Alice')");
    $rows = queryRows($r, "INSERT INTO users (email, name) VALUES ('a@test.com', 'Bob') ON CONFLICT (email) DO UPDATE SET name = 'Updated' RETURNING name");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Updated');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// RETURNING — UPDATE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('RETURNING — UPDATE');

test('UPDATE RETURNING *', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE items (name TEXT, price INTEGER)');
    sql($r, "INSERT INTO items (name, price) VALUES ('Widget', 25)");
    sql($r, "INSERT INTO items (name, price) VALUES ('Gadget', 50)");
    $rows = queryRows($r, "UPDATE items SET price = price + 10 WHERE name = 'Widget' RETURNING *");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Widget');
    assertEqual($rows[0]['price'], 35);
});

test('UPDATE RETURNING specific columns', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE items (name TEXT, price INTEGER)');
    sql($r, "INSERT INTO items (name, price) VALUES ('Widget', 25)");
    $rows = queryRows($r, "UPDATE items SET price = 99 RETURNING name, price");
    assertCount(1, $rows);
    assertEqual($rows[0]['price'], 99);
    assertTrue(!isset($rows[0]['id']));
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// RETURNING — DELETE
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('RETURNING — DELETE');

test('DELETE RETURNING *', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE items (name TEXT, price INTEGER)');
    sql($r, "INSERT INTO items (name, price) VALUES ('Widget', 25)");
    sql($r, "INSERT INTO items (name, price) VALUES ('Gadget', 50)");
    $rows = queryRows($r, "DELETE FROM items WHERE name = 'Widget' RETURNING *");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Widget');
    // Verify row was actually deleted
    $remaining = queryRows($r, 'SELECT * FROM items');
    assertCount(1, $remaining);
});

test('DELETE RETURNING specific columns', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE items (name TEXT, price INTEGER)');
    sql($r, "INSERT INTO items (name, price) VALUES ('Widget', 25)");
    $rows = queryRows($r, "DELETE FROM items WHERE name = 'Widget' RETURNING id, name");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Widget');
    assertTrue(!isset($rows[0]['price']));
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MULTI-JOIN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Multi-JOIN');

test('three-table JOIN', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE departments (name TEXT)');
    sql($r, "INSERT INTO departments (name) VALUES ('Engineering')");
    sql($r, "INSERT INTO departments (name) VALUES ('Marketing')");
    sql($r, 'CREATE TABLE employees (name TEXT, dept TEXT)');
    sql($r, "INSERT INTO employees (name, dept) VALUES ('Alice', 'Engineering')");
    sql($r, "INSERT INTO employees (name, dept) VALUES ('Bob', 'Marketing')");
    sql($r, 'CREATE TABLE projects (title TEXT, dept TEXT)');
    sql($r, "INSERT INTO projects (title, dept) VALUES ('Project X', 'Engineering')");
    sql($r, "INSERT INTO projects (title, dept) VALUES ('Campaign A', 'Marketing')");

    $rows = queryRows($r, "SELECT e.name, p.title FROM employees e JOIN departments d ON e.dept = d.name JOIN projects p ON p.dept = d.name ORDER BY e.name");
    assertCount(2, $rows);
    assertEqual($rows[0]['name'], 'Alice');
    assertEqual($rows[0]['title'], 'Project X');
    assertEqual($rows[1]['name'], 'Bob');
    assertEqual($rows[1]['title'], 'Campaign A');
});

test('three-table JOIN with filter', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE a (val TEXT)');
    sql($r, "INSERT INTO a (val) VALUES ('x')");
    sql($r, "INSERT INTO a (val) VALUES ('y')");
    sql($r, 'CREATE TABLE b (val TEXT, extra INTEGER)');
    sql($r, "INSERT INTO b (val, extra) VALUES ('x', 10)");
    sql($r, "INSERT INTO b (val, extra) VALUES ('y', 20)");
    sql($r, 'CREATE TABLE c (val TEXT, info TEXT)');
    sql($r, "INSERT INTO c (val, info) VALUES ('x', 'hello')");
    sql($r, "INSERT INTO c (val, info) VALUES ('y', 'world')");

    $rows = queryRows($r, "SELECT a.val, b.extra, c.info FROM a JOIN b ON a.val = b.val JOIN c ON a.val = c.val WHERE b.extra > 15");
    assertCount(1, $rows);
    assertEqual($rows[0]['val'], 'y');
    assertEqual($rows[0]['extra'], 20);
    assertEqual($rows[0]['info'], 'world');
});

test('LEFT JOIN + INNER JOIN chained', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE orders (id INTEGER, customer TEXT)');
    sql($r, "INSERT INTO orders (id, customer) VALUES (1, 'Alice')");
    sql($r, "INSERT INTO orders (id, customer) VALUES (2, 'Bob')");
    sql($r, 'CREATE TABLE items (order_id INTEGER, product TEXT)');
    sql($r, "INSERT INTO items (order_id, product) VALUES (1, 'Widget')");
    sql($r, 'CREATE TABLE reviews (product TEXT, rating INTEGER)');
    sql($r, "INSERT INTO reviews (product, rating) VALUES ('Widget', 5)");

    $rows = queryRows($r, "SELECT o.customer, i.product, r.rating FROM orders o LEFT JOIN items i ON o.id = i.order_id LEFT JOIN reviews r ON i.product = r.product ORDER BY o.customer");
    assertCount(2, $rows);
    $alice = $rows[0];
    assertEqual($alice['customer'], 'Alice');
    assertEqual($alice['product'], 'Widget');
    assertEqual($alice['rating'], 5);
    $bob = $rows[1];
    assertEqual($bob['customer'], 'Bob');
    assertNull($bob['product']);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// INTEGRATION
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Integration');

test('UPSERT + RETURNING + expression', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE counters (name TEXT, value INTEGER)');
    sql($r, "INSERT INTO counters (name, value) VALUES ('hits', 0)");
    $rows = queryRows($r, "INSERT INTO counters (name, value) VALUES ('hits', 0) ON CONFLICT (name) DO UPDATE SET value = value + 1 RETURNING name, value");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'hits');
    assertEqual($rows[0]['value'], 1);
});

test('CTE + multi-JOIN', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE departments (name TEXT, budget INTEGER)');
    sql($r, "INSERT INTO departments (name, budget) VALUES ('Engineering', 500000)");
    sql($r, 'CREATE TABLE employees (name TEXT, dept TEXT, salary INTEGER)');
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Alice', 'Engineering', 120000)");
    sql($r, 'CREATE TABLE projects (title TEXT, dept TEXT)');
    sql($r, "INSERT INTO projects (title, dept) VALUES ('Project X', 'Engineering')");

    $rows = queryRows($r, "WITH big_depts AS (SELECT name FROM departments WHERE budget > 300000) SELECT e.name, p.title FROM employees e JOIN big_depts bd ON e.dept = bd.name JOIN projects p ON p.dept = bd.name");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Alice');
    assertEqual($rows[0]['title'], 'Project X');
});

report();
