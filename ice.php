#!/usr/bin/env php
<?php
/**
 * ICE Database — Interactive SQL Shell.
 *
 * Usage:
 *   php ice.php                    # In-memory database
 *   php ice.php --data ./mydb      # File-backed, persistent
 *   php ice.php --data ./mydb -e "SELECT * FROM users"   # One-shot
 *
 * Type SQL at the prompt. Semicolons are optional.
 * Special commands:
 *   .tables     — list all tables
 *   .schema T   — show schema for table T
 *   .quit       — exit
 *   .help       — show this help
 */
require __DIR__ . '/ice/autoload.php';

use Ice\Core\Event;
use Ice\Persistence\MemoryStore;
use Ice\Persistence\MemoryRefs;
use Ice\Persistence\FileStore;
use Ice\Persistence\FileRefs;
use Ice\Resolution\Runner;

use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;

// ─── Parse CLI args ──────────────────────────────────────
$dataDir = null;
$execSql = null;
$args = array_slice($argv, 1);
for ($i = 0; $i < count($args); $i++) {
    if ($args[$i] === '--data' || $args[$i] === '-d') {
        $dataDir = $args[++$i] ?? null;
    } elseif ($args[$i] === '-e') {
        $execSql = $args[++$i] ?? null;
    } elseif ($args[$i] === '--help' || $args[$i] === '-h') {
        echo "Usage: php ice.php [--data DIR] [-e SQL]\n";
        echo "  --data DIR   Use file-backed persistence in DIR\n";
        echo "  -e SQL       Execute SQL and exit\n";
        exit(0);
    }
}

// ─── Create runner ───────────────────────────────────────
if ($dataDir !== null) {
    if (!is_dir($dataDir)) mkdir($dataDir, 0755, true);
    $store = new FileStore($dataDir);
    $refs = new FileRefs($dataDir);
} else {
    $store = new MemoryStore();
    $refs = new MemoryRefs();
}

$runner = new Runner($store, $refs);
registerDatabaseGates($runner);
registerSQLGates($runner);

// ─── Execute SQL helper ──────────────────────────────────
function execSQL(Runner $runner, string $sql): void {
    $runner->clearPending();
    $runner->emit(new Event('sql', ['sql' => $sql]));
    $pending = $runner->sample()['pending'];

    foreach ($pending as $event) {
        switch ($event->type) {
            case 'query_result':
                printTable($event->data['rows']);
                break;
            case 'error':
                fwrite(STDERR, "ERROR: {$event->data['message']}\n");
                break;
            case 'table_created':
                echo "Table created.\n";
                break;
            case 'table_dropped':
                echo "Table dropped.\n";
                break;
            case 'row_inserted':
                echo "Inserted row {$event->data['id']}.\n";
                break;
            case 'row_updated':
                $cnt = count($event->data['ids'] ?? []);
                echo "Updated {$cnt} row(s).\n";
                break;
            case 'row_deleted':
                $cnt = count($event->data['ids'] ?? []);
                echo "Deleted {$cnt} row(s).\n";
                break;
            case 'index_created':
                echo "Index created.\n";
                break;
            case 'index_dropped':
                echo "Index dropped.\n";
                break;
            case 'view_created':
                echo "View created.\n";
                break;
            case 'view_dropped':
                echo "View dropped.\n";
                break;
            case 'trigger_created':
                echo "Trigger created.\n";
                break;
            case 'trigger_dropped':
                echo "Trigger dropped.\n";
                break;
            case 'constraint_created':
                echo "Constraint created.\n";
                break;
            case 'constraint_dropped':
                echo "Constraint dropped.\n";
                break;
            case 'column_added':
                echo "Column '{$event->data['column']}' added.\n";
                break;
            case 'column_dropped':
                echo "Column '{$event->data['column']}' dropped.\n";
                break;
            case 'table_renamed':
                echo "Table renamed: {$event->data['oldName']} → {$event->data['newName']}.\n";
                break;
            case 'transaction_begun':
                echo "Transaction started.\n";
                break;
            case 'transaction_committed':
                echo "Transaction committed.\n";
                break;
            case 'transaction_rolled_back':
                echo "Transaction rolled back.\n";
                break;
            // Silently ignore internal events like scan_result etc.
        }
    }
}

function printTable(array $rows): void {
    if (count($rows) === 0) {
        echo "(0 rows)\n";
        return;
    }

    // Collect all column names
    $cols = [];
    foreach ($rows as $row) {
        foreach (array_keys($row) as $k) {
            if (!in_array($k, $cols)) $cols[] = $k;
        }
    }

    // Compute column widths
    $widths = [];
    foreach ($cols as $col) {
        $widths[$col] = strlen($col);
    }
    foreach ($rows as $row) {
        foreach ($cols as $col) {
            $val = formatVal($row[$col] ?? null);
            $widths[$col] = max($widths[$col], strlen($val));
        }
    }

    // Header
    $sep = '+';
    $header = '|';
    foreach ($cols as $col) {
        $sep .= '-' . str_repeat('-', $widths[$col]) . '-+';
        $header .= ' ' . str_pad($col, $widths[$col]) . ' |';
    }

    echo "$sep\n$header\n$sep\n";

    // Rows
    foreach ($rows as $row) {
        $line = '|';
        foreach ($cols as $col) {
            $val = formatVal($row[$col] ?? null);
            $line .= ' ' . str_pad($val, $widths[$col]) . ' |';
        }
        echo "$line\n";
    }
    echo "$sep\n";
    echo '(' . count($rows) . " row" . (count($rows) !== 1 ? 's' : '') . ")\n";
}

function formatVal(mixed $v): string {
    if ($v === null) return 'NULL';
    if ($v === true) return 'TRUE';
    if ($v === false) return 'FALSE';
    return (string)$v;
}

// ─── Dot-commands ────────────────────────────────────────
function handleDotCommand(Runner $runner, string $cmd): bool {
    $parts = preg_split('/\s+/', trim($cmd), 2);
    $command = strtolower($parts[0]);
    $arg = $parts[1] ?? null;

    switch ($command) {
        case '.quit':
        case '.exit':
            echo "Bye.\n";
            exit(0);

        case '.help':
            echo "  .tables        List all tables\n";
            echo "  .schema TABLE  Show table schema\n";
            echo "  .quit          Exit\n";
            echo "  .help          Show this help\n";
            return true;

        case '.tables':
            $refs = $runner->getRefs();
            $names = $refs->list('db/tables/');
            $tables = [];
            foreach ($names as $name) {
                if (preg_match('#^db/tables/([^/]+)/schema$#', $name, $m)) {
                    $tables[] = $m[1];
                }
            }
            if (count($tables) === 0) {
                echo "(no tables)\n";
            } else {
                foreach ($tables as $t) echo "  $t\n";
            }
            return true;

        case '.schema':
            if (!$arg) {
                echo "Usage: .schema TABLE\n";
                return true;
            }
            $hash = $runner->getRefs()->get("db/tables/$arg/schema");
            if ($hash === null) {
                echo "Table '$arg' not found.\n";
                return true;
            }
            $schema = $runner->getStore()->get($hash);
            echo "CREATE TABLE $arg (\n";
            foreach ($schema['columns'] as $i => $col) {
                $line = "  {$col['name']} {$col['type']}";
                if (isset($col['nullable']) && $col['nullable'] === false) $line .= ' NOT NULL';
                if (isset($col['default']) && $col['default'] !== null) $line .= " DEFAULT " . json_encode($col['default']);
                if ($i < count($schema['columns']) - 1) $line .= ',';
                echo "$line\n";
            }
            echo ");\n";
            return true;

        default:
            echo "Unknown command: $command. Type .help for help.\n";
            return true;
    }
}

// ─── One-shot mode ───────────────────────────────────────
if ($execSql !== null) {
    execSQL($runner, $execSql);
    exit(0);
}

// ─── Interactive REPL ────────────────────────────────────
$mode = $dataDir ? "file:$dataDir" : 'memory';
echo "ICE Database v0.9 ($mode)\n";
echo "Type .help for commands, SQL at the prompt.\n\n";

$buffer = '';
while (true) {
    $prompt = $buffer === '' ? 'ice> ' : '...> ';
    if (function_exists('readline')) {
        $line = readline($prompt);
        if ($line === false) { echo "\nBye.\n"; break; }
        if (trim($line) !== '') readline_add_history($line);
    } else {
        echo $prompt;
        $line = fgets(STDIN);
        if ($line === false) { echo "\nBye.\n"; break; }
        $line = rtrim($line, "\n\r");
    }

    $trimmed = trim($line);

    // Dot commands (only when buffer is empty)
    if ($buffer === '' && str_starts_with($trimmed, '.')) {
        handleDotCommand($runner, $trimmed);
        continue;
    }

    // Accumulate multi-line input
    $buffer .= ($buffer !== '' ? "\n" : '') . $line;

    // Execute on semicolon or complete single-statement
    if (str_contains($buffer, ';') || $buffer !== '' && !str_contains($buffer, '(')) {
        $statements = explode(';', $buffer);
        foreach ($statements as $stmt) {
            $stmt = trim($stmt);
            if ($stmt === '') continue;
            execSQL($runner, $stmt);
        }
        $buffer = '';
    }
}
