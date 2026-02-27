<?php
/**
 * ICE Database — Phase 14 Test Suite
 * CTEs, Window Functions, Derived Tables, COUNT DISTINCT, GROUP_CONCAT.
 */
require __DIR__ . '/runner.php';
require __DIR__ . '/../ice/autoload.php';

use Ice\Core\Event;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Resolution\Runner;

use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;

echo "ICE Database — Phase 14: CTEs, Window Functions & More\n";

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

function setupData(Runner $r): void {
    sql($r, 'CREATE TABLE employees (name TEXT, dept TEXT, salary INTEGER)');
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Alice', 'Engineering', 120000)");
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Bob', 'Marketing', 80000)");
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Charlie', 'Engineering', 110000)");
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Diana', 'Sales', 90000)");
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Eve', 'Marketing', 75000)");
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Frank', 'Engineering', 130000)");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// COMMON TABLE EXPRESSIONS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Common Table Expressions');

test('simple CTE', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "WITH eng AS (SELECT name, salary FROM employees WHERE dept = 'Engineering') SELECT name FROM eng WHERE salary > 115000");
    assertCount(2, $rows); // Alice (120k), Frank (130k)
});

test('CTE with aggregation', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "WITH dept_avg AS (SELECT dept, AVG(salary) AS avg_sal FROM employees GROUP BY dept) SELECT dept, avg_sal FROM dept_avg WHERE avg_sal > 85000");
    assertCount(2, $rows); // Engineering avg=120k, Sales avg=90k
});

test('multiple CTEs', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "WITH high AS (SELECT name FROM employees WHERE salary > 100000), low AS (SELECT name FROM employees WHERE salary < 80000) SELECT name FROM high");
    assertCount(3, $rows); // Alice, Charlie, Frank
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// DERIVED TABLES
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Derived tables');

test('basic derived table', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT t.name FROM (SELECT name, salary FROM employees WHERE salary > 100000) AS t ORDER BY t.name");
    assertCount(3, $rows);
    assertEqual($rows[0]['name'], 'Alice');
});

test('derived table with aggregation', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT d.dept FROM (SELECT dept, COUNT(*) AS cnt FROM employees GROUP BY dept) AS d WHERE d.cnt > 1 ORDER BY d.dept");
    assertCount(2, $rows); // Engineering (3), Marketing (2)
    assertEqual($rows[0]['dept'], 'Engineering');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// WINDOW FUNCTIONS — ROW_NUMBER, RANK, DENSE_RANK
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Window functions — ranking');

test('ROW_NUMBER() OVER (ORDER BY ...)', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM employees");
    assertCount(6, $rows);
    // First row should be Frank (130k) with rn=1
    $frank = array_values(array_filter($rows, fn($r) => $r['name'] === 'Frank'));
    assertEqual($frank[0]['rn'], 1);
});

test('ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM employees");
    // Engineering: Frank=1, Alice=2, Charlie=3
    $frank = array_values(array_filter($rows, fn($r) => $r['name'] === 'Frank'));
    assertEqual($frank[0]['rn'], 1);
    $alice = array_values(array_filter($rows, fn($r) => $r['name'] === 'Alice'));
    assertEqual($alice[0]['rn'], 2);
});

test('RANK() with ties', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE scores (name TEXT, score INTEGER)');
    sql($r, "INSERT INTO scores (name, score) VALUES ('A', 100)");
    sql($r, "INSERT INTO scores (name, score) VALUES ('B', 90)");
    sql($r, "INSERT INTO scores (name, score) VALUES ('C', 90)");
    sql($r, "INSERT INTO scores (name, score) VALUES ('D', 80)");
    $rows = queryRows($r, "SELECT name, RANK() OVER (ORDER BY score DESC) AS rnk FROM scores");
    $a = array_values(array_filter($rows, fn($r) => $r['name'] === 'A'));
    $b = array_values(array_filter($rows, fn($r) => $r['name'] === 'B'));
    $c = array_values(array_filter($rows, fn($r) => $r['name'] === 'C'));
    $d = array_values(array_filter($rows, fn($r) => $r['name'] === 'D'));
    assertEqual($a[0]['rnk'], 1);
    assertEqual($b[0]['rnk'], 2);
    assertEqual($c[0]['rnk'], 2); // Tied with B
    assertEqual($d[0]['rnk'], 4); // Skips 3
});

test('DENSE_RANK() with ties', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE scores (name TEXT, score INTEGER)');
    sql($r, "INSERT INTO scores (name, score) VALUES ('A', 100)");
    sql($r, "INSERT INTO scores (name, score) VALUES ('B', 90)");
    sql($r, "INSERT INTO scores (name, score) VALUES ('C', 90)");
    sql($r, "INSERT INTO scores (name, score) VALUES ('D', 80)");
    $rows = queryRows($r, "SELECT name, DENSE_RANK() OVER (ORDER BY score DESC) AS rnk FROM scores");
    $d = array_values(array_filter($rows, fn($r) => $r['name'] === 'D'));
    assertEqual($d[0]['rnk'], 3); // No gap — 1,2,2,3
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// WINDOW FUNCTIONS — aggregates
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Window functions — aggregates');

test('SUM() OVER (PARTITION BY ...)', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name, dept, SUM(salary) OVER (PARTITION BY dept) AS dept_total FROM employees WHERE dept = 'Engineering'");
    assertCount(3, $rows);
    assertEqual($rows[0]['dept_total'], 360000); // 120k + 110k + 130k
});

test('COUNT() OVER ()', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name, COUNT(*) OVER () AS total FROM employees");
    assertCount(6, $rows);
    assertEqual($rows[0]['total'], 6);
});

test('AVG() OVER (PARTITION BY ...)', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name, dept, AVG(salary) OVER (PARTITION BY dept) AS dept_avg FROM employees WHERE dept = 'Marketing'");
    assertCount(2, $rows);
    assertEqual($rows[0]['dept_avg'], 77500); // (80k + 75k) / 2
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// COUNT DISTINCT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('COUNT DISTINCT');

test('COUNT(DISTINCT col)', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT COUNT(DISTINCT dept) AS dept_count FROM employees");
    assertCount(1, $rows);
    assertEqual($rows[0]['dept_count'], 3);
});

test('COUNT(DISTINCT col) with GROUP BY', function () {
    $r = freshRunner();
    setupData($r);
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Grace', 'Engineering', 120000)");
    $rows = queryRows($r, "SELECT dept, COUNT(DISTINCT salary) AS unique_sals FROM employees GROUP BY dept ORDER BY dept");
    // Engineering: 120k, 110k, 130k, 120k → 3 distinct
    $eng = array_values(array_filter($rows, fn($r) => $r['dept'] === 'Engineering'));
    assertEqual($eng[0]['unique_sals'], 3);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// GROUP_CONCAT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('GROUP_CONCAT');

test('GROUP_CONCAT basic', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT dept, GROUP_CONCAT(name) AS names FROM employees GROUP BY dept ORDER BY dept");
    assertCount(3, $rows);
    $eng = array_values(array_filter($rows, fn($r) => $r['dept'] === 'Engineering'));
    // Should contain Alice, Charlie, Frank in some order
    $names = explode(',', $eng[0]['names']);
    assertCount(3, $names);
});

test('GROUP_CONCAT with SEPARATOR', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT dept, GROUP_CONCAT(name SEPARATOR ' | ') AS names FROM employees WHERE dept = 'Marketing' GROUP BY dept");
    assertCount(1, $rows);
    assertTrue(str_contains($rows[0]['names'], ' | '));
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// INTEGRATION
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Integration');

test('CTE + window function', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "WITH ranked AS (SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM employees) SELECT name, salary FROM ranked WHERE rn <= 3");
    assertCount(3, $rows);
});

test('derived table + window function', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT t.name FROM (SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM employees) AS t WHERE t.rn = 1");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Frank');
});

report();
