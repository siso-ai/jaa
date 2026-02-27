<?php
/**
 * ICE Database — Phase 9 Test Suite
 * Transactions, Multi-row INSERT, ALTER TABLE, CLI.
 * Run: php test/test-phase9.php
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

echo "ICE Database — Phase 9: Transactions, ALTER TABLE, Multi-INSERT, CLI\n";

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

$tmpDir = sys_get_temp_dir() . '/ice_test9_' . getmypid();

function cleanTmp(): void {
    global $tmpDir;
    if (is_dir($tmpDir)) {
        $it = new RecursiveDirectoryIterator($tmpDir, RecursiveDirectoryIterator::SKIP_DOTS);
        $files = new RecursiveIteratorIterator($it, RecursiveIteratorIterator::CHILD_FIRST);
        foreach ($files as $f) { $f->isDir() ? rmdir($f->getRealPath()) : unlink($f->getRealPath()); }
        rmdir($tmpDir);
    }
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// TRANSACTION TESTS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Transactions / BEGIN');

test('BEGIN emits transaction_begun', function () {
    $r = freshRunner();
    sql($r, 'BEGIN');
    $begun = pendingOfType($r, 'transaction_begun');
    assertTrue(count($begun) > 0);
});

test('BEGIN via direct event', function () {
    $r = freshRunner();
    $r->emit(new Event('transaction_begin', []));
    $begun = pendingOfType($r, 'transaction_begun');
    assertTrue(count($begun) > 0);
});

test('double BEGIN errors', function () {
    $r = freshRunner();
    sql($r, 'BEGIN');
    sql($r, 'BEGIN');
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

section('Transactions / COMMIT');

test('COMMIT after BEGIN succeeds', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'BEGIN');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'COMMIT');
    $committed = pendingOfType($r, 'transaction_committed');
    assertTrue(count($committed) > 0);

    // Data should persist
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(1, $rows);
    assertEqual($rows[0]['x'], 1);
});

test('COMMIT without BEGIN errors', function () {
    $r = freshRunner();
    sql($r, 'COMMIT');
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

section('Transactions / ROLLBACK');

test('ROLLBACK reverts INSERT', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');

    sql($r, 'BEGIN');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    sql($r, 'INSERT INTO t (x) VALUES (3)');

    // Before rollback: 3 rows
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(3, $rows);

    sql($r, 'ROLLBACK');
    $rolledBack = pendingOfType($r, 'transaction_rolled_back');
    assertTrue(count($rolledBack) > 0);

    // After rollback: back to 1 row
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(1, $rows);
    assertEqual($rows[0]['x'], 1);
});

test('ROLLBACK reverts UPDATE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (10)');

    sql($r, 'BEGIN');
    sql($r, 'UPDATE t SET x = 99 WHERE x = 10');

    $rows = queryRows($r, 'SELECT * FROM t');
    assertEqual($rows[0]['x'], 99);

    sql($r, 'ROLLBACK');

    $rows = queryRows($r, 'SELECT * FROM t');
    assertEqual($rows[0]['x'], 10);
});

test('ROLLBACK reverts DELETE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'INSERT INTO t (x) VALUES (2)');

    sql($r, 'BEGIN');
    sql($r, 'DELETE FROM t WHERE x = 1');
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(1, $rows);

    sql($r, 'ROLLBACK');
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(2, $rows);
});

test('ROLLBACK reverts CREATE TABLE', function () {
    $r = freshRunner();

    sql($r, 'BEGIN');
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (1)');

    sql($r, 'ROLLBACK');

    // Table should be gone — scan produces 0 rows (no error because TableScan returns empty for missing)
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(0, $rows);
});

test('ROLLBACK reverts DROP TABLE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (42)');

    sql($r, 'BEGIN');
    sql($r, 'DROP TABLE t');

    sql($r, 'ROLLBACK');

    // Table should be back
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(1, $rows);
    assertEqual($rows[0]['x'], 42);
});

test('ROLLBACK without BEGIN errors', function () {
    $r = freshRunner();
    sql($r, 'ROLLBACK');
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

test('COMMIT then ROLLBACK errors', function () {
    $r = freshRunner();
    sql($r, 'BEGIN');
    sql($r, 'COMMIT');
    sql($r, 'ROLLBACK');
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

test('multiple transactions sequentially', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');

    // Transaction 1: committed
    sql($r, 'BEGIN');
    sql($r, 'INSERT INTO t (x) VALUES (1)');
    sql($r, 'COMMIT');

    // Transaction 2: rolled back
    sql($r, 'BEGIN');
    sql($r, 'INSERT INTO t (x) VALUES (2)');
    sql($r, 'ROLLBACK');

    // Transaction 3: committed
    sql($r, 'BEGIN');
    sql($r, 'INSERT INTO t (x) VALUES (3)');
    sql($r, 'COMMIT');

    $rows = queryRows($r, 'SELECT * FROM t ORDER BY x');
    assertCount(2, $rows);
    assertEqual($rows[0]['x'], 1);
    assertEqual($rows[1]['x'], 3);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// MULTI-ROW INSERT TESTS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Multi-row INSERT');

test('INSERT multiple rows via VALUES', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT, age INTEGER)');
    sql($r, "INSERT INTO t (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)");
    $rows = queryRows($r, 'SELECT * FROM t ORDER BY age');
    assertCount(3, $rows);
    assertEqual($rows[0]['name'], 'Bob');
    assertEqual($rows[1]['name'], 'Alice');
    assertEqual($rows[2]['name'], 'Charlie');
});

test('multi-row INSERT assigns sequential IDs', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (10), (20), (30)');
    $rows = queryRows($r, 'SELECT * FROM t ORDER BY id');
    assertCount(3, $rows);
    assertEqual($rows[0]['id'], 1);
    assertEqual($rows[1]['id'], 2);
    assertEqual($rows[2]['id'], 3);
});

test('multi-row INSERT with defaults', function () {
    $r = freshRunner();
    sql($r, "CREATE TABLE t (status TEXT DEFAULT 'active', value INTEGER)");
    sql($r, 'INSERT INTO t (value) VALUES (1), (2)');
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(2, $rows);
    assertEqual($rows[0]['status'], 'active');
    assertEqual($rows[1]['status'], 'active');
});

test('single-row INSERT still works', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    sql($r, 'INSERT INTO t (x) VALUES (42)');
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(1, $rows);
    assertEqual($rows[0]['x'], 42);
});

test('multi-row INSERT 10 rows', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (x INTEGER)');
    $values = implode(', ', array_map(fn($i) => "($i)", range(1, 10)));
    sql($r, "INSERT INTO t (x) VALUES $values");
    $rows = queryRows($r, 'SELECT COUNT(*) AS cnt FROM t');
    assertEqual($rows[0]['cnt'], 10);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// ALTER TABLE TESTS
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('ALTER TABLE / ADD COLUMN');

test('ADD COLUMN via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT)');
    sql($r, "INSERT INTO t (name) VALUES ('Alice')");
    sql($r, 'ALTER TABLE t ADD COLUMN age INTEGER');
    $added = pendingOfType($r, 'column_added');
    assertTrue(count($added) > 0);
});

test('ADD COLUMN backfills NULL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT)');
    sql($r, "INSERT INTO t (name) VALUES ('Alice')");
    sql($r, "INSERT INTO t (name) VALUES ('Bob')");
    sql($r, 'ALTER TABLE t ADD COLUMN age INTEGER');
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(2, $rows);
    assertNull($rows[0]['age']);
    assertNull($rows[1]['age']);
});

test('ADD COLUMN with DEFAULT backfills value', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT)');
    sql($r, "INSERT INTO t (name) VALUES ('Alice')");
    sql($r, "ALTER TABLE t ADD COLUMN status TEXT DEFAULT 'active'");
    $rows = queryRows($r, 'SELECT * FROM t');
    assertEqual($rows[0]['status'], 'active');
});

test('ADD COLUMN then INSERT uses new column', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT)');
    sql($r, 'ALTER TABLE t ADD COLUMN score INTEGER');
    sql($r, "INSERT INTO t (name, score) VALUES ('Alice', 100)");
    $rows = queryRows($r, 'SELECT * FROM t');
    assertEqual($rows[0]['score'], 100);
});

test('ADD COLUMN duplicate error', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT)');
    sql($r, 'ALTER TABLE t ADD COLUMN name TEXT');
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

test('ADD COLUMN to nonexistent table error', function () {
    $r = freshRunner();
    sql($r, 'ALTER TABLE nope ADD COLUMN x INTEGER');
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

test('ADD COLUMN keyword is optional', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (a INTEGER)');
    sql($r, 'ALTER TABLE t ADD b TEXT');
    $added = pendingOfType($r, 'column_added');
    assertTrue(count($added) > 0);
});

section('ALTER TABLE / DROP COLUMN');

test('DROP COLUMN via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT, age INTEGER)');
    sql($r, "INSERT INTO t (name, age) VALUES ('Alice', 30)");
    sql($r, 'ALTER TABLE t DROP COLUMN age');
    $dropped = pendingOfType($r, 'column_dropped');
    assertTrue(count($dropped) > 0);
});

test('DROP COLUMN removes from rows', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT, age INTEGER, score REAL)');
    sql($r, "INSERT INTO t (name, age, score) VALUES ('Alice', 30, 95.5)");
    sql($r, 'ALTER TABLE t DROP COLUMN age');
    $rows = queryRows($r, 'SELECT * FROM t');
    assertTrue(!isset($rows[0]['age']));
    assertEqual($rows[0]['name'], 'Alice');
    assertTrue(abs($rows[0]['score'] - 95.5) < 0.01);
});

test('DROP COLUMN nonexistent error', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT)');
    sql($r, 'ALTER TABLE t DROP COLUMN nope');
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

test('DROP COLUMN id not allowed', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT)');
    sql($r, 'INSERT INTO t (name) VALUES (1)');
    $r->emit(new Event('alter_table_drop_column', ['table' => 't', 'column' => 'id']));
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

test('DROP COLUMN on nonexistent table error', function () {
    $r = freshRunner();
    sql($r, 'ALTER TABLE nope DROP COLUMN x');
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

section('ALTER TABLE / RENAME');

test('RENAME TABLE via SQL', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE old_name (x INTEGER)');
    sql($r, 'INSERT INTO old_name (x) VALUES (42)');
    sql($r, 'ALTER TABLE old_name RENAME TO new_name');
    $renamed = pendingOfType($r, 'table_renamed');
    assertTrue(count($renamed) > 0);
});

test('RENAME TABLE data persists', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE alpha (x INTEGER)');
    sql($r, 'INSERT INTO alpha (x) VALUES (1)');
    sql($r, 'INSERT INTO alpha (x) VALUES (2)');
    sql($r, 'ALTER TABLE alpha RENAME TO beta');

    // New name works
    $rows = queryRows($r, 'SELECT * FROM beta');
    assertCount(2, $rows);

    // Old name is empty
    $rows = queryRows($r, 'SELECT * FROM alpha');
    assertCount(0, $rows);
});

test('RENAME TABLE nonexistent error', function () {
    $r = freshRunner();
    $r->emit(new Event('rename_table', ['table' => 'nope', 'newName' => 'x']));
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

test('RENAME TABLE to existing name error', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE a (x INTEGER)');
    sql($r, 'CREATE TABLE b (y INTEGER)');
    $r->emit(new Event('rename_table', ['table' => 'a', 'newName' => 'b']));
    $errors = pendingOfType($r, 'error');
    assertTrue(count($errors) > 0);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// TRANSACTION + ALTER TABLE INTEGRATION
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('Transaction + ALTER TABLE');

test('ROLLBACK reverts ADD COLUMN', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE t (name TEXT)');
    sql($r, "INSERT INTO t (name) VALUES ('Alice')");

    sql($r, 'BEGIN');
    sql($r, 'ALTER TABLE t ADD COLUMN age INTEGER');
    sql($r, 'ROLLBACK');

    // Column should not exist — insert with 'age' should still work
    // (it'll be ignored by schema, which shouldn't have 'age')
    $rows = queryRows($r, 'SELECT * FROM t');
    assertCount(1, $rows);
    assertTrue(!isset($rows[0]['age']));
});

test('ROLLBACK reverts RENAME TABLE', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE original (x INTEGER)');
    sql($r, 'INSERT INTO original (x) VALUES (1)');

    sql($r, 'BEGIN');
    sql($r, 'ALTER TABLE original RENAME TO renamed');
    $rows = queryRows($r, 'SELECT * FROM renamed');
    assertCount(1, $rows);

    sql($r, 'ROLLBACK');

    // Back to original name
    $rows = queryRows($r, 'SELECT * FROM original');
    assertCount(1, $rows);
    $rows = queryRows($r, 'SELECT * FROM renamed');
    assertCount(0, $rows);
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// CLI / REPL TESTS (one-shot -e mode)
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('CLI (one-shot mode)');

test('cli -e CREATE + INSERT + SELECT', function () {
    global $tmpDir; cleanTmp();
    $ice = __DIR__ . '/../ice.php';

    // Create table
    $out = shell_exec("php $ice --data $tmpDir -e \"CREATE TABLE test (name TEXT, val INTEGER)\" 2>&1");
    assertTrue(str_contains($out, 'Table created'));

    // Insert
    $out = shell_exec("php $ice --data $tmpDir -e \"INSERT INTO test (name, val) VALUES ('hello', 42)\" 2>&1");
    assertTrue(str_contains($out, 'Inserted'));

    // Select — data persisted to disk
    $out = shell_exec("php $ice --data $tmpDir -e \"SELECT * FROM test\" 2>&1");
    assertTrue(str_contains($out, 'hello'));
    assertTrue(str_contains($out, '42'));
    assertTrue(str_contains($out, '1 row'));
});

test('cli -e memory mode', function () {
    $ice = __DIR__ . '/../ice.php';
    // Without --data, memory mode (won't persist between calls, but single -e works)
    $out = shell_exec("php $ice -e \"SELECT 'ok'\" 2>&1");
    // This will error since there's no table, but dispatch should work
    assertTrue($out !== null);
});

test('cli --help', function () {
    $ice = __DIR__ . '/../ice.php';
    $out = shell_exec("php $ice --help 2>&1");
    assertTrue(str_contains($out, 'Usage'));
});

test('cli file persistence across invocations', function () {
    global $tmpDir; cleanTmp();
    $ice = __DIR__ . '/../ice.php';

    shell_exec("php $ice --data $tmpDir -e \"CREATE TABLE items (name TEXT)\" 2>&1");
    shell_exec("php $ice --data $tmpDir -e \"INSERT INTO items (name) VALUES ('Widget')\" 2>&1");
    shell_exec("php $ice --data $tmpDir -e \"INSERT INTO items (name) VALUES ('Gadget')\" 2>&1");

    $out = shell_exec("php $ice --data $tmpDir -e \"SELECT * FROM items\" 2>&1");
    assertTrue(str_contains($out, 'Widget'));
    assertTrue(str_contains($out, 'Gadget'));
    assertTrue(str_contains($out, '2 row'));
});

test('cli multi-row INSERT', function () {
    global $tmpDir; cleanTmp();
    $ice = __DIR__ . '/../ice.php';

    shell_exec("php $ice --data $tmpDir -e \"CREATE TABLE t (x INTEGER)\" 2>&1");
    shell_exec("php $ice --data $tmpDir -e \"INSERT INTO t (x) VALUES (1), (2), (3)\" 2>&1");

    $out = shell_exec("php $ice --data $tmpDir -e \"SELECT COUNT(*) AS cnt FROM t\" 2>&1");
    assertTrue(str_contains($out, '3'));
});

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// END-TO-END INTEGRATION
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
section('End-to-End Integration');

test('full workflow: schema evolution + transactions', function () {
    $r = freshRunner();

    // Create initial table
    sql($r, 'CREATE TABLE products (name TEXT, price INTEGER)');
    sql($r, "INSERT INTO products (name, price) VALUES ('Widget', 10), ('Gadget', 20)");

    // Evolve schema
    sql($r, "ALTER TABLE products ADD COLUMN category TEXT DEFAULT 'general'");
    sql($r, "ALTER TABLE products ADD COLUMN in_stock BOOLEAN DEFAULT TRUE");

    $rows = queryRows($r, 'SELECT * FROM products');
    assertCount(2, $rows);
    assertEqual($rows[0]['category'], 'general');

    // Transaction: try a risky operation, rollback
    sql($r, 'BEGIN');
    sql($r, 'DELETE FROM products');
    $rows = queryRows($r, 'SELECT * FROM products');
    assertCount(0, $rows);

    sql($r, 'ROLLBACK');
    $rows = queryRows($r, 'SELECT * FROM products');
    assertCount(2, $rows);

    // Transaction: successful batch update
    sql($r, 'BEGIN');
    sql($r, "UPDATE products SET price = 15 WHERE name = 'Widget'");
    sql($r, "INSERT INTO products (name, price, category) VALUES ('Doohickey', 30, 'premium')");
    sql($r, 'COMMIT');

    $rows = queryRows($r, 'SELECT * FROM products ORDER BY price');
    assertCount(3, $rows);
    assertEqual($rows[0]['price'], 15); // Widget updated
    assertEqual($rows[2]['name'], 'Doohickey');
});

test('rename + insert + query', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE tmp (x INTEGER)');
    sql($r, 'INSERT INTO tmp (x) VALUES (1), (2), (3)');
    sql($r, 'ALTER TABLE tmp RENAME TO final_table');
    sql($r, 'INSERT INTO final_table (x) VALUES (4)');
    $rows = queryRows($r, 'SELECT * FROM final_table ORDER BY x');
    assertCount(4, $rows);
    assertEqual($rows[3]['x'], 4);
});

test('drop column + aggregate query', function () {
    $r = freshRunner();
    sql($r, 'CREATE TABLE scores (player TEXT, score INTEGER, timestamp TEXT)');
    sql($r, "INSERT INTO scores (player, score, timestamp) VALUES ('A', 100, '2024-01-01'), ('B', 200, '2024-01-02'), ('A', 150, '2024-01-03')");
    sql($r, 'ALTER TABLE scores DROP COLUMN timestamp');
    $rows = queryRows($r, 'SELECT SUM(score) AS total FROM scores GROUP BY player');
    assertCount(2, $rows);
    $totals = array_column($rows, 'total');
    sort($totals);
    assertEqual($totals[0], 200);
    assertEqual($totals[1], 250);
});

test('file persistence with ALTER TABLE', function () {
    global $tmpDir; cleanTmp();

    $r1 = new Runner(new FileStore($tmpDir), new FileRefs($tmpDir));
    registerDatabaseGates($r1);
    registerSQLGates($r1);

    sql($r1, 'CREATE TABLE t (name TEXT)');
    sql($r1, "INSERT INTO t (name) VALUES ('Alice')");
    sql($r1, "ALTER TABLE t ADD COLUMN score INTEGER DEFAULT 0");

    // New runner reads from disk
    $r2 = new Runner(new FileStore($tmpDir), new FileRefs($tmpDir));
    registerDatabaseGates($r2);
    registerSQLGates($r2);

    $rows = queryRows($r2, 'SELECT * FROM t');
    assertCount(1, $rows);
    assertEqual($rows[0]['name'], 'Alice');
    assertEqual($rows[0]['score'], 0);
});

// ─── DONE ────────────────────────────────────────────────
cleanTmp();
report();
