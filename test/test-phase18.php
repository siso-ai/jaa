<?php
/**
 * ICE Database — Phase 18 Test Suite
 * SELECT without FROM, ORDER BY column number, INSERT DEFAULT VALUES,
 * multi-condition JOIN ON (AND), nested subqueries.
 */
require __DIR__ . '/runner.php';
require __DIR__ . '/../ice/autoload.php';

use Ice\Core\Event;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Resolution\Runner;

use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;

echo "ICE Database — Phase 18: SELECT w/o FROM, ORDER BY #, DEFAULT VALUES, multi-ON\n";

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
section('SELECT without FROM');

test('SELECT literal expression', function () {
    $r = freshRunner();
    $rows = queryRows($r, 'SELECT 1 + 1 AS result');
    assertCount(1, $rows);
    assertEqual($rows[0]['result'], 2);
});

test('SELECT string function without FROM', function () {
    $r = freshRunner();
    $rows = queryRows($r, "SELECT UPPER('hello') AS greeting");
    assertEqual($rows[0]['greeting'], 'HELLO');
});

test('SELECT multiple expressions', function () {
    $r = freshRunner();
    $rows = queryRows($r, "SELECT 2 * 3 AS product, 10 - 4 AS diff, LOWER('ABC') AS lower_abc");
    assertEqual($rows[0]['product'], 6);
    assertEqual($rows[0]['diff'], 6);
    assertEqual($rows[0]['lower_abc'], 'abc');
});

test('SELECT PI()', function () {
    $r = freshRunner();
    $rows = queryRows($r, 'SELECT PI() AS pi');
    assertTrue(abs($rows[0]['pi'] - 3.14159) < 0.001);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('ORDER BY column number');

test('ORDER BY 1 ASC', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE emp (name TEXT, salary INTEGER)');
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Carol', 150)");
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Alice', 100)");
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Bob', 200)");
    $rows = queryRows($r, 'SELECT name, salary FROM emp ORDER BY 1');
    assertEqual($rows[0]['name'], 'Alice');
    assertEqual($rows[1]['name'], 'Bob');
    assertEqual($rows[2]['name'], 'Carol');
});

test('ORDER BY 2 DESC', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE emp (name TEXT, salary INTEGER)');
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Alice', 100)");
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Bob', 200)");
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Carol', 150)");
    $rows = queryRows($r, 'SELECT name, salary FROM emp ORDER BY 2 DESC');
    assertEqual($rows[0]['name'], 'Bob');
    assertEqual($rows[1]['name'], 'Carol');
    assertEqual($rows[2]['name'], 'Alice');
});

test('ORDER BY 1, 2 mixed', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE data (cat TEXT, val INTEGER)');
    sql($r, "INSERT INTO data (cat, val) VALUES ('B', 2)");
    sql($r, "INSERT INTO data (cat, val) VALUES ('A', 3)");
    sql($r, "INSERT INTO data (cat, val) VALUES ('A', 1)");
    $rows = queryRows($r, 'SELECT cat, val FROM data ORDER BY 1, 2');
    assertEqual($rows[0]['cat'], 'A');
    assertEqual($rows[0]['val'], 1);
    assertEqual($rows[1]['cat'], 'A');
    assertEqual($rows[1]['val'], 3);
    assertEqual($rows[2]['cat'], 'B');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('INSERT DEFAULT VALUES');

test('INSERT DEFAULT VALUES uses column defaults', function () {
    $r = freshRunner();
    sql($r, "CREATE TABLE log (msg TEXT DEFAULT 'auto', level INTEGER DEFAULT 0)");
    sql($r, 'INSERT INTO log DEFAULT VALUES');
    $rows = queryRows($r, 'SELECT * FROM log');
    assertCount(1, $rows);
    assertEqual($rows[0]['msg'], 'auto');
    assertEqual($rows[0]['level'], 0);
});

test('INSERT DEFAULT VALUES with nullable columns', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE notes (title TEXT, body TEXT)');
    sql($r, 'INSERT INTO notes DEFAULT VALUES');
    $rows = queryRows($r, 'SELECT * FROM notes');
    assertCount(1, $rows);
    assertNull($rows[0]['title']);
    assertNull($rows[0]['body']);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Multi-condition JOIN ON (AND)');

test('JOIN ON two conditions', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE orders (oid INTEGER, customer TEXT, region TEXT)');
    sql($r, "INSERT INTO orders (oid, customer, region) VALUES (1, 'Alice', 'East')");
    sql($r, "INSERT INTO orders (oid, customer, region) VALUES (2, 'Bob', 'West')");
    sql($r, "INSERT INTO orders (oid, customer, region) VALUES (3, 'Alice', 'West')");
    sql($r, 'CREATE TABLE reps (name TEXT, region TEXT)');
    sql($r, "INSERT INTO reps (name, region) VALUES ('Alice', 'East')");
    sql($r, "INSERT INTO reps (name, region) VALUES ('Carol', 'West')");
    $rows = queryRows($r, 'SELECT o.oid FROM orders o JOIN reps r ON o.customer = r.name AND o.region = r.region');
    assertCount(1, $rows);
    assertEqual($rows[0]['oid'], 1);
});

test('LEFT JOIN ON two conditions', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE a (x INTEGER, y INTEGER)');
    sql($r, 'INSERT INTO a (x, y) VALUES (1, 10)');
    sql($r, 'INSERT INTO a (x, y) VALUES (2, 20)');
    sql($r, 'CREATE TABLE b (x INTEGER, y INTEGER, label TEXT)');
    sql($r, "INSERT INTO b (x, y, label) VALUES (1, 10, 'match')");
    sql($r, "INSERT INTO b (x, y, label) VALUES (2, 99, 'no')");
    $rows = queryRows($r, 'SELECT t1.x, t2.label FROM a t1 LEFT JOIN b t2 ON t1.x = t2.x AND t1.y = t2.y ORDER BY t1.x');
    assertCount(2, $rows);
    assertEqual($rows[0]['label'], 'match');
    assertNull($rows[1]['label']);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Nested subqueries');

test('Subquery inside subquery in WHERE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE emp (name TEXT, salary INTEGER)');
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Alice', 100)");
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Bob', 200)");
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Carol', 150)");
    // salary > AVG of those above MIN
    $rows = queryRows($r, 'SELECT name FROM emp WHERE salary > (SELECT AVG(salary) FROM emp WHERE salary > (SELECT MIN(salary) FROM emp))');
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Bob');
});

test('Nested IN subqueries', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE dept (name TEXT, active INTEGER)');
    sql($r, "INSERT INTO dept (name, active) VALUES ('Eng', 1)");
    sql($r, "INSERT INTO dept (name, active) VALUES ('Sales', 0)");
    sql($r, "INSERT INTO dept (name, active) VALUES ('HR', 1)");
    sql($r, 'CREATE TABLE emp (name TEXT, dept TEXT)');
    sql($r, "INSERT INTO emp (name, dept) VALUES ('Alice', 'Eng')");
    sql($r, "INSERT INTO emp (name, dept) VALUES ('Bob', 'Sales')");
    sql($r, "INSERT INTO emp (name, dept) VALUES ('Carol', 'HR')");
    sql($r, 'CREATE TABLE projects (emp_name TEXT, project TEXT)');
    sql($r, "INSERT INTO projects (emp_name, project) VALUES ('Alice', 'Alpha')");
    sql($r, "INSERT INTO projects (emp_name, project) VALUES ('Carol', 'Beta')");
    // projects of employees in active departments
    $rows = queryRows($r, "SELECT project FROM projects WHERE emp_name IN (SELECT name FROM emp WHERE dept IN (SELECT name FROM dept WHERE active = 1)) ORDER BY project");
    assertCount(2, $rows);
    assertEqual($rows[0]['project'], 'Alpha');
    assertEqual($rows[1]['project'], 'Beta');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Integration');

test('SELECT without FROM in CTE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE emp (name TEXT, salary INTEGER)');
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Alice', 100)");
    sql($r, "INSERT INTO emp (name, salary) VALUES ('Bob', 200)");
    $rows = queryRows($r, 'WITH threshold AS (SELECT 150 AS val) SELECT e.name FROM emp e CROSS JOIN threshold t WHERE e.salary > t.val');
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Bob');
});

test('ORDER BY column number with expression alias', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE items (name TEXT, price INTEGER, qty INTEGER)');
    sql($r, "INSERT INTO items (name, price, qty) VALUES ('A', 10, 5)");
    sql($r, "INSERT INTO items (name, price, qty) VALUES ('B', 20, 3)");
    sql($r, "INSERT INTO items (name, price, qty) VALUES ('C', 5, 10)");
    $rows = queryRows($r, 'SELECT name, price * qty AS total FROM items ORDER BY 2 DESC');
    assertEqual($rows[0]['name'], 'B'); // 60
    assertEqual($rows[1]['name'], 'A'); // 50
    assertEqual($rows[2]['name'], 'C'); // 50
});

test('Multi-ON JOIN + aggregate + ORDER BY number', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE sales (region TEXT, product TEXT, amount INTEGER)');
    sql($r, "INSERT INTO sales (region, product, amount) VALUES ('East', 'Widget', 100)");
    sql($r, "INSERT INTO sales (region, product, amount) VALUES ('East', 'Widget', 200)");
    sql($r, "INSERT INTO sales (region, product, amount) VALUES ('West', 'Gadget', 500)");
    sql($r, 'CREATE TABLE targets (region TEXT, product TEXT, target INTEGER)');
    sql($r, "INSERT INTO targets (region, product, target) VALUES ('East', 'Widget', 250)");
    sql($r, "INSERT INTO targets (region, product, target) VALUES ('West', 'Gadget', 100)");
    $rows = queryRows($r, 'SELECT s.region, s.product, SUM(s.amount) AS total, t.target FROM sales s JOIN targets t ON s.region = t.region AND s.product = t.product GROUP BY s.region, s.product, t.target ORDER BY 3 DESC');
    assertCount(2, $rows);
    assertEqual($rows[0]['region'], 'West');  // total 500
    assertEqual($rows[0]['total'], 500);
    assertEqual($rows[1]['region'], 'East');  // total 300
    assertEqual($rows[1]['total'], 300);
});

report();
