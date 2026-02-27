<?php
/**
 * ICE Database — Phase 19 Test Suite
 * IIF, IFNULL, UNION+ORDER BY+LIMIT, UPDATE FROM, date/time functions.
 */
require __DIR__ . '/runner.php';
require __DIR__ . '/../ice/autoload.php';

use Ice\Core\Event;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Resolution\Runner;

use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;

echo "ICE Database — Phase 19: IIF, UPDATE FROM, UNION ORDER, Date/Time\n";

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
section('IIF function');

test('IIF true branch', function () {
    $r = freshRunner();
    $rows = queryRows($r, 'SELECT IIF(1 > 0, 10, 20) AS val');
    assertEqual($rows[0]['val'], 10);
});

test('IIF false branch', function () {
    $r = freshRunner();
    $rows = queryRows($r, 'SELECT IIF(1 = 2, 10, 20) AS val');
    assertEqual($rows[0]['val'], 20);
});

test('IIF with column values', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE scores (name TEXT, score INTEGER)');
    sql($r, "INSERT INTO scores (name, score) VALUES ('Alice', 90)");
    sql($r, "INSERT INTO scores (name, score) VALUES ('Bob', 60)");
    $rows = queryRows($r, "SELECT name, IIF(score >= 70, 'pass', 'fail') AS result FROM scores ORDER BY name");
    assertEqual($rows[0]['result'], 'pass');  // Alice
    assertEqual($rows[1]['result'], 'fail');  // Bob
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('IFNULL function');

test('IFNULL returns first non-null', function () {
    $r = freshRunner();
    $rows = queryRows($r, 'SELECT IFNULL(NULL, 42) AS val');
    assertEqual($rows[0]['val'], 42);
});

test('IFNULL returns first arg if not null', function () {
    $r = freshRunner();
    $rows = queryRows($r, 'SELECT IFNULL(10, 42) AS val');
    assertEqual($rows[0]['val'], 10);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('UNION with ORDER BY and LIMIT');

test('UNION ALL + ORDER BY', function () {
    $r = freshRunner();
    $rows = queryRows($r, 'SELECT 3 AS n UNION ALL SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n');
    assertCount(3, $rows);
    assertEqual($rows[0]['n'], 1);
    assertEqual($rows[1]['n'], 2);
    assertEqual($rows[2]['n'], 3);
});

test('UNION ALL + ORDER BY + LIMIT', function () {
    $r = freshRunner();
    $rows = queryRows($r, 'SELECT 3 AS n UNION ALL SELECT 1 AS n UNION ALL SELECT 2 AS n ORDER BY n LIMIT 2');
    assertCount(2, $rows);
    assertEqual($rows[0]['n'], 1);
    assertEqual($rows[1]['n'], 2);
});

test('UNION + ORDER BY DESC', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE a (x INTEGER)');
    sql($r, 'INSERT INTO a (x) VALUES (1)');
    sql($r, 'INSERT INTO a (x) VALUES (3)');
    sql($r, 'CREATE TABLE b (x INTEGER)');
    sql($r, 'INSERT INTO b (x) VALUES (2)');
    sql($r, 'INSERT INTO b (x) VALUES (4)');
    $rows = queryRows($r, 'SELECT x FROM a UNION ALL SELECT x FROM b ORDER BY x DESC');
    assertCount(4, $rows);
    assertEqual($rows[0]['x'], 4);
    assertEqual($rows[3]['x'], 1);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('UPDATE FROM (join update)');

test('UPDATE FROM basic join', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE emp (name TEXT, salary INTEGER, dept TEXT)');
    sql($r, "INSERT INTO emp (name, salary, dept) VALUES ('Alice', 100, 'Eng')");
    sql($r, "INSERT INTO emp (name, salary, dept) VALUES ('Bob', 200, 'Sales')");
    sql($r, 'CREATE TABLE bonuses (dept TEXT, bonus INTEGER)');
    sql($r, "INSERT INTO bonuses (dept, bonus) VALUES ('Eng', 50)");
    sql($r, "INSERT INTO bonuses (dept, bonus) VALUES ('Sales', 100)");
    sql($r, 'UPDATE emp SET salary = salary + bonuses.bonus FROM bonuses WHERE emp.dept = bonuses.dept');
    $rows = queryRows($r, 'SELECT name, salary FROM emp ORDER BY name');
    assertEqual($rows[0]['salary'], 150);  // Alice: 100+50
    assertEqual($rows[1]['salary'], 300);  // Bob: 200+100
});

test('UPDATE FROM partial match', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE products (name TEXT, price INTEGER, category TEXT)');
    sql($r, "INSERT INTO products (name, price, category) VALUES ('Widget', 10, 'A')");
    sql($r, "INSERT INTO products (name, price, category) VALUES ('Gadget', 20, 'B')");
    sql($r, "INSERT INTO products (name, price, category) VALUES ('Bolt', 5, 'C')");
    sql($r, 'CREATE TABLE price_changes (category TEXT, adjustment INTEGER)');
    sql($r, "INSERT INTO price_changes (category, adjustment) VALUES ('A', 5)");
    sql($r, "INSERT INTO price_changes (category, adjustment) VALUES ('B', -3)");
    sql($r, 'UPDATE products SET price = price + price_changes.adjustment FROM price_changes WHERE products.category = price_changes.category');
    $rows = queryRows($r, 'SELECT name, price FROM products ORDER BY name');
    assertEqual($rows[0]['price'], 5);   // Bolt: unchanged
    assertEqual($rows[1]['price'], 17);  // Gadget: 20-3
    assertEqual($rows[2]['price'], 15);  // Widget: 10+5
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Date/Time functions');

test('DATE returns valid date format', function () {
    $r = freshRunner();
    $rows = queryRows($r, "SELECT DATE('now') AS d");
    assertTrue(preg_match('/^\d{4}-\d{2}-\d{2}$/', $rows[0]['d']) === 1);
});

test('TIME returns valid time format', function () {
    $r = freshRunner();
    $rows = queryRows($r, "SELECT TIME('now') AS t");
    assertTrue(preg_match('/^\d{2}:\d{2}:\d{2}$/', $rows[0]['t']) === 1);
});

test('DATETIME returns valid datetime format', function () {
    $r = freshRunner();
    $rows = queryRows($r, "SELECT DATETIME('now') AS dt");
    assertTrue(preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $rows[0]['dt']) === 1);
});

test('CURRENT_DATE returns date', function () {
    $r = freshRunner();
    $rows = queryRows($r, "SELECT CURRENT_DATE() AS d");
    assertTrue(preg_match('/^\d{4}-\d{2}-\d{2}$/', $rows[0]['d']) === 1);
});

test('DATE with specific value', function () {
    $r = freshRunner();
    $rows = queryRows($r, "SELECT DATE('2024-06-15 14:30:00') AS d");
    assertEqual($rows[0]['d'], '2024-06-15');
});

test('STRFTIME format', function () {
    $r = freshRunner();
    $rows = queryRows($r, "SELECT STRFTIME('%Y', '2024-06-15') AS y");
    assertEqual($rows[0]['y'], '2024');
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Integration');

test('IIF in SELECT with WHERE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE orders (item TEXT, amount INTEGER)');
    sql($r, 'INSERT INTO orders (item, amount) VALUES (\'A\', 100)');
    sql($r, 'INSERT INTO orders (item, amount) VALUES (\'B\', 200)');
    sql($r, 'INSERT INTO orders (item, amount) VALUES (\'C\', 50)');
    $rows = queryRows($r, "SELECT item, IIF(amount >= 100, 'big', 'small') AS size FROM orders ORDER BY item");
    assertEqual($rows[0]['size'], 'big');   // A: 100
    assertEqual($rows[1]['size'], 'big');   // B: 200
    assertEqual($rows[2]['size'], 'small'); // C: 50
});

test('UNION ALL + ORDER BY from tables', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE cats (name TEXT, age INTEGER)');
    sql($r, "INSERT INTO cats (name, age) VALUES ('Whiskers', 5)");
    sql($r, "INSERT INTO cats (name, age) VALUES ('Mittens', 3)");
    sql($r, 'CREATE TABLE dogs (name TEXT, age INTEGER)');
    sql($r, "INSERT INTO dogs (name, age) VALUES ('Buddy', 7)");
    sql($r, "INSERT INTO dogs (name, age) VALUES ('Rex', 2)");
    $rows = queryRows($r, 'SELECT name, age FROM cats UNION ALL SELECT name, age FROM dogs ORDER BY age');
    assertCount(4, $rows);
    assertEqual($rows[0]['name'], 'Rex');     // age 2
    assertEqual($rows[3]['name'], 'Buddy');   // age 7
});

test('UPDATE FROM + RETURNING', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE emp (name TEXT, salary INTEGER, dept TEXT)');
    sql($r, "INSERT INTO emp (name, salary, dept) VALUES ('Alice', 100, 'Eng')");
    sql($r, "INSERT INTO emp (name, salary, dept) VALUES ('Bob', 200, 'Sales')");
    sql($r, 'CREATE TABLE raises (dept TEXT, amount INTEGER)');
    sql($r, "INSERT INTO raises (dept, amount) VALUES ('Eng', 25)");
    $rows = queryRows($r, 'UPDATE emp SET salary = salary + raises.amount FROM raises WHERE emp.dept = raises.dept RETURNING name, salary');
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Alice');
    assertEqual($rows[0]['salary'], 125);
});

report();
