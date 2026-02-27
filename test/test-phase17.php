<?php
/**
 * ICE Database — Phase 17 Test Suite
 * CROSS JOIN, HAVING aggregates, subqueries in UPDATE/DELETE WHERE, ORDER BY index.
 */
require __DIR__ . '/runner.php';
require __DIR__ . '/../ice/autoload.php';

use Ice\Core\Event;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Resolution\Runner;

use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;

echo "ICE Database — Phase 17: CROSS JOIN, HAVING, Subqueries in DML\n";

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

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('CROSS JOIN');

test('CROSS JOIN produces cartesian product', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE colors (c TEXT)');
    sql($r, "INSERT INTO colors (c) VALUES ('red')");
    sql($r, "INSERT INTO colors (c) VALUES ('blue')");
    sql($r, 'CREATE TABLE sizes (s TEXT)');
    sql($r, "INSERT INTO sizes (s) VALUES ('S')");
    sql($r, "INSERT INTO sizes (s) VALUES ('M')");
    sql($r, "INSERT INTO sizes (s) VALUES ('L')");
    $rows = queryRows($r, 'SELECT c, s FROM colors CROSS JOIN sizes ORDER BY c, s');
    assertCount(6, $rows);
    assertEqual($rows[0]['c'], 'blue');
    assertEqual($rows[0]['s'], 'L');
});

test('CROSS JOIN with aliases', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE a (x INTEGER)');
    sql($r, 'INSERT INTO a (x) VALUES (1)');
    sql($r, 'INSERT INTO a (x) VALUES (2)');
    sql($r, 'CREATE TABLE b (y INTEGER)');
    sql($r, 'INSERT INTO b (y) VALUES (10)');
    sql($r, 'INSERT INTO b (y) VALUES (20)');
    $rows = queryRows($r, 'SELECT t1.x, t2.y FROM a t1 CROSS JOIN b t2 ORDER BY t1.x, t2.y');
    assertCount(4, $rows);
    assertEqual($rows[0]['x'], 1);
    assertEqual($rows[0]['y'], 10);
    assertEqual($rows[3]['x'], 2);
    assertEqual($rows[3]['y'], 20);
});

test('CROSS JOIN with WHERE filter', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE a (x INTEGER)');
    sql($r, 'INSERT INTO a (x) VALUES (1)');
    sql($r, 'INSERT INTO a (x) VALUES (2)');
    sql($r, 'INSERT INTO a (x) VALUES (3)');
    sql($r, 'CREATE TABLE b (y INTEGER)');
    sql($r, 'INSERT INTO b (y) VALUES (2)');
    sql($r, 'INSERT INTO b (y) VALUES (3)');
    $rows = queryRows($r, 'SELECT a.x, b.y FROM a CROSS JOIN b WHERE a.x = b.y');
    assertCount(2, $rows);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('HAVING with aggregate expressions');

test('HAVING SUM() > value', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE sales (dept TEXT, amount INTEGER)');
    sql($r, "INSERT INTO sales (dept, amount) VALUES ('A', 100)");
    sql($r, "INSERT INTO sales (dept, amount) VALUES ('A', 200)");
    sql($r, "INSERT INTO sales (dept, amount) VALUES ('B', 50)");
    $rows = queryRows($r, 'SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept HAVING SUM(amount) > 100');
    assertCount(1, $rows);
    assertEqual($rows[0]['dept'], 'A');
    assertEqual($rows[0]['total'], 300);
});

test('HAVING COUNT(*) > value', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE items (category TEXT, name TEXT)');
    sql($r, "INSERT INTO items (category, name) VALUES ('Tools', 'Hammer')");
    sql($r, "INSERT INTO items (category, name) VALUES ('Tools', 'Wrench')");
    sql($r, "INSERT INTO items (category, name) VALUES ('Tools', 'Drill')");
    sql($r, "INSERT INTO items (category, name) VALUES ('Paint', 'Red')");
    $rows = queryRows($r, 'SELECT category, COUNT(*) AS cnt FROM items GROUP BY category HAVING COUNT(*) > 1');
    assertCount(1, $rows);
    assertEqual($rows[0]['category'], 'Tools');
    assertEqual($rows[0]['cnt'], 3);
});

test('HAVING AVG() with comparison', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE scores (team TEXT, score INTEGER)');
    sql($r, "INSERT INTO scores (team, score) VALUES ('A', 80)");
    sql($r, "INSERT INTO scores (team, score) VALUES ('A', 90)");
    sql($r, "INSERT INTO scores (team, score) VALUES ('B', 50)");
    sql($r, "INSERT INTO scores (team, score) VALUES ('B', 60)");
    $rows = queryRows($r, 'SELECT team, AVG(score) AS avg_score FROM scores GROUP BY team HAVING AVG(score) >= 80');
    assertCount(1, $rows);
    assertEqual($rows[0]['team'], 'A');
});

test('HAVING with alias still works', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE sales (dept TEXT, amount INTEGER)');
    sql($r, "INSERT INTO sales (dept, amount) VALUES ('A', 100)");
    sql($r, "INSERT INTO sales (dept, amount) VALUES ('A', 200)");
    sql($r, "INSERT INTO sales (dept, amount) VALUES ('B', 50)");
    $rows = queryRows($r, 'SELECT dept, SUM(amount) AS total FROM sales GROUP BY dept HAVING total > 100');
    assertCount(1, $rows);
    assertEqual($rows[0]['dept'], 'A');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Subquery in UPDATE WHERE');

test('UPDATE with IN subquery', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE emp (name TEXT, dept TEXT, salary INTEGER)');
    sql($r, "INSERT INTO emp (name, dept, salary) VALUES ('Alice', 'Eng', 100)");
    sql($r, "INSERT INTO emp (name, dept, salary) VALUES ('Bob', 'Sales', 200)");
    sql($r, "INSERT INTO emp (name, dept, salary) VALUES ('Carol', 'Eng', 150)");
    sql($r, "UPDATE emp SET salary = salary + 50 WHERE name IN (SELECT name FROM emp WHERE salary > 140)");
    $rows = queryRows($r, 'SELECT name, salary FROM emp ORDER BY name');
    assertEqual($rows[0]['salary'], 100);   // Alice: unchanged
    assertEqual($rows[1]['salary'], 250);   // Bob: 200+50
    assertEqual($rows[2]['salary'], 200);   // Carol: 150+50
});

test('UPDATE with EXISTS subquery', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE products (name TEXT, price INTEGER)');
    sql($r, 'CREATE TABLE discounts (product_name TEXT, discount INTEGER)');
    sql($r, "INSERT INTO products (name, price) VALUES ('Widget', 100)");
    sql($r, "INSERT INTO products (name, price) VALUES ('Gadget', 200)");
    sql($r, "INSERT INTO discounts (product_name, discount) VALUES ('Widget', 10)");
    sql($r, "UPDATE products SET price = price - 10 WHERE EXISTS (SELECT 1 FROM discounts WHERE product_name = 'Widget')");
    // EXISTS is not correlated — applies to all rows if true
    $rows = queryRows($r, 'SELECT name, price FROM products ORDER BY name');
    assertEqual($rows[0]['price'], 190);  // Gadget
    assertEqual($rows[1]['price'], 90);   // Widget
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Subquery in DELETE WHERE');

test('DELETE with IN subquery', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE items (name TEXT, price INTEGER)');
    sql($r, "INSERT INTO items (name, price) VALUES ('Cheap', 5)");
    sql($r, "INSERT INTO items (name, price) VALUES ('Mid', 50)");
    sql($r, "INSERT INTO items (name, price) VALUES ('Expensive', 500)");
    sql($r, "DELETE FROM items WHERE name IN (SELECT name FROM items WHERE price > 100)");
    $rows = queryRows($r, 'SELECT name FROM items ORDER BY name');
    assertCount(2, $rows);
    assertEqual($rows[0]['name'], 'Cheap');
    assertEqual($rows[1]['name'], 'Mid');
});

test('DELETE with NOT IN subquery', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE emp (name TEXT, dept TEXT)');
    sql($r, 'CREATE TABLE active_depts (dept TEXT)');
    sql($r, "INSERT INTO emp (name, dept) VALUES ('Alice', 'Eng')");
    sql($r, "INSERT INTO emp (name, dept) VALUES ('Bob', 'Sales')");
    sql($r, "INSERT INTO emp (name, dept) VALUES ('Carol', 'HR')");
    sql($r, "INSERT INTO active_depts (dept) VALUES ('Eng')");
    sql($r, "INSERT INTO active_depts (dept) VALUES ('Sales')");
    sql($r, "DELETE FROM emp WHERE dept NOT IN (SELECT dept FROM active_depts)");
    $rows = queryRows($r, 'SELECT name FROM emp ORDER BY name');
    assertCount(2, $rows);
    assertEqual($rows[0]['name'], 'Alice');
    assertEqual($rows[1]['name'], 'Bob');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Self-join');

test('Self-join to find pairs', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE emp (name TEXT, salary INTEGER)');
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Alice', 100)");
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Bob', 200)");
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Carol', 150)");
    $rows = queryRows($r, 'SELECT a.name AS lower, b.name AS higher FROM emp a JOIN emp b ON a.salary < b.salary ORDER BY a.name, b.name');
    assertCount(3, $rows); // Alice<Bob, Alice<Carol, Carol<Bob
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Integration');

test('CROSS JOIN + aggregate + HAVING', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE products (name TEXT, price INTEGER)');
    sql($r, "INSERT INTO products (name, price) VALUES ('A', 10)");
    sql($r, "INSERT INTO products (name, price) VALUES ('B', 20)");
    sql($r, 'CREATE TABLE regions (region TEXT)');
    sql($r, "INSERT INTO regions (region) VALUES ('East')");
    sql($r, "INSERT INTO regions (region) VALUES ('West')");
    $rows = queryRows($r, 'SELECT region, SUM(price) AS total FROM products CROSS JOIN regions GROUP BY region HAVING SUM(price) > 20');
    assertCount(2, $rows); // Both regions get sum 30
});

test('Subquery UPDATE + RETURNING', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE emp (name TEXT, salary INTEGER, dept TEXT)');
    sql($r, "INSERT INTO emp (name, salary, dept) VALUES ('Alice', 100, 'Eng')");
    sql($r, "INSERT INTO emp (name, salary, dept) VALUES ('Bob', 200, 'Sales')");
    $rows = queryRows($r, "UPDATE emp SET salary = salary * 2 WHERE dept IN (SELECT dept FROM emp WHERE salary > 150) RETURNING name, salary");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Bob');
    assertEqual($rows[0]['salary'], 400);
});

test('DELETE subquery + CTE source', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE emp (name TEXT, salary INTEGER)');
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Alice', 100)");
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Bob', 200)");
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Carol', 300)");
    // Use subquery referencing same table
    sql($r, "DELETE FROM emp WHERE salary IN (SELECT salary FROM emp WHERE salary > 200)");
    $rows = queryRows($r, 'SELECT name FROM emp ORDER BY name');
    assertCount(2, $rows);
    assertEqual($rows[0]['name'], 'Alice');
    assertEqual($rows[1]['name'], 'Bob');
});

report();
