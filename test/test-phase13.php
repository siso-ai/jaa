<?php
/**
 * ICE Database — Phase 13 Test Suite
 * Subqueries (IN, EXISTS, scalar), Table Aliases.
 */
require __DIR__ . '/runner.php';
require __DIR__ . '/../ice/autoload.php';

use Ice\Core\Event;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Resolution\Runner;

use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;

echo "ICE Database — Phase 13: Subqueries & Table Aliases\n";

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

function hasError(Runner $r): bool {
    return count(array_filter($r->sample()['pending'], fn($e) => $e->type === 'error')) > 0;
}

function setupData(Runner $r): void {
    sql($r, 'CREATE TABLE departments (name TEXT, budget INTEGER)');
    sql($r, "INSERT INTO departments (name, budget) VALUES ('Engineering', 500000)");
    sql($r, "INSERT INTO departments (name, budget) VALUES ('Marketing', 200000)");
    sql($r, "INSERT INTO departments (name, budget) VALUES ('Sales', 300000)");

    sql($r, 'CREATE TABLE employees (name TEXT, dept TEXT, salary INTEGER)');
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Alice', 'Engineering', 120000)");
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Bob', 'Marketing', 80000)");
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Charlie', 'Engineering', 110000)");
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Diana', 'Sales', 90000)");
    sql($r, "INSERT INTO employees (name, dept, salary) VALUES ('Eve', 'Marketing', 75000)");
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// IN SUBQUERY
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('IN subquery');

test('WHERE col IN (SELECT ...)', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name FROM employees WHERE dept IN (SELECT name FROM departments WHERE budget > 250000)");
    assertCount(3, $rows); // Engineering (Alice, Charlie) + Sales (Diana)
});

test('WHERE col NOT IN (SELECT ...)', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name FROM employees WHERE dept NOT IN (SELECT name FROM departments WHERE budget > 250000)");
    assertCount(2, $rows); // Marketing (Bob, Eve)
});

test('IN subquery returns single column', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name FROM departments WHERE name IN (SELECT dept FROM employees WHERE salary > 100000)");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Engineering');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// EXISTS SUBQUERY
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('EXISTS subquery');

test('WHERE EXISTS (SELECT ...)', function () {
    $r = freshRunner();
    setupData($r);
    // EXISTS returns true if subquery has any rows
    $rows = queryRows($r, "SELECT name FROM departments WHERE EXISTS (SELECT name FROM employees WHERE salary > 100000)");
    assertCount(3, $rows); // All departments, since the subquery has results
});

test('WHERE NOT EXISTS (SELECT ...)', function () {
    $r = freshRunner();
    setupData($r);
    // NOT EXISTS returns true if subquery returns no rows
    $rows = queryRows($r, "SELECT name FROM departments WHERE NOT EXISTS (SELECT name FROM employees WHERE salary > 200000)");
    assertCount(3, $rows); // All departments, since nobody earns > 200000
});

test('EXISTS with empty result', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name FROM departments WHERE EXISTS (SELECT name FROM employees WHERE salary > 999999)");
    assertCount(0, $rows); // Nobody earns that much
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SCALAR SUBQUERY IN SELECT
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Scalar subquery in SELECT');

test('scalar subquery in SELECT list', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name, (SELECT MAX(salary) FROM employees) AS max_sal FROM departments WHERE name = 'Engineering'");
    assertCount(1, $rows);
    assertEqual($rows[0]['max_sal'], 120000);
});

test('scalar subquery returns NULL when empty', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name, (SELECT salary FROM employees WHERE salary > 999999) AS big_sal FROM departments WHERE name = 'Engineering'");
    assertCount(1, $rows);
    assertNull($rows[0]['big_sal']);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// TABLE ALIASES
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Table aliases');

test('FROM table alias', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT e.name FROM employees e WHERE e.salary > 100000");
    assertCount(2, $rows);
});

test('FROM table AS alias', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT e.name FROM employees AS e WHERE e.salary > 100000");
    assertCount(2, $rows);
});

test('JOIN with aliases', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT e.name, d.budget FROM employees e JOIN departments d ON e.dept = d.name WHERE d.budget > 250000 ORDER BY e.name");
    assertCount(3, $rows);
    assertEqual($rows[0]['name'], 'Alice');
    assertEqual($rows[0]['budget'], 500000);
});

test('alias in ORDER BY', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT e.name, e.salary FROM employees e ORDER BY e.salary DESC");
    assertEqual($rows[0]['name'], 'Alice');
    assertEqual($rows[0]['salary'], 120000);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// COMBINED FEATURES
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Combined features');

test('IN subquery with expression', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT name, salary FROM employees WHERE salary IN (SELECT MAX(salary) FROM employees)");
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Alice');
});

test('nested subqueries', function () {
    $r = freshRunner();
    setupData($r);
    // Employees in departments with budgets above the min budget
    $rows = queryRows($r, "SELECT name FROM employees WHERE dept IN (SELECT name FROM departments WHERE budget > (SELECT MIN(budget) FROM departments))");
    assertCount(3, $rows); // Engineering + Sales (but not Marketing which has min budget)
});

test('scalar subquery + table alias + WHERE', function () {
    $r = freshRunner();
    setupData($r);
    $rows = queryRows($r, "SELECT e.name, e.salary, (SELECT MAX(salary) FROM employees) AS top_sal FROM employees e WHERE e.salary > 100000");
    assertCount(2, $rows);
    assertEqual($rows[0]['top_sal'], 120000);
});

test('EXISTS + alias', function () {
    $r = freshRunner();
    setupData($r);
    // Non-correlated EXISTS: shows all departments if any high-salary employees exist
    $rows = queryRows($r, "SELECT d.name FROM departments d WHERE EXISTS (SELECT name FROM employees WHERE salary > 100000)");
    assertCount(3, $rows);
});

report();
