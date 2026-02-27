<?php
/**
 * jää Database — JSON API
 *
 * POST /api.php   body: {"sql":"..."}   → execute SQL
 * GET  /api.php?schema=1                → return schema
 *
 * Debug payload always returned under "debug":
 *   debug.stream  — StreamLog entries (full event flow through gates)
 *   debug.php     — captured PHP warnings/notices
 *   debug.memory  — peak memory in KB
 *   debug.time    — server wall time in ms
 */
header('Content-Type: application/json');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');

if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') { http_response_code(204); exit; }

// ── Capture PHP errors ───────────────────────────
$phpErrors = [];
set_error_handler(function ($severity, $message, $file, $line) use (&$phpErrors) {
    $levels = [
        E_WARNING => 'WARNING', E_NOTICE => 'NOTICE',
        E_STRICT => 'STRICT', E_DEPRECATED => 'DEPRECATED',
        E_USER_WARNING => 'WARNING', E_USER_NOTICE => 'NOTICE',
    ];
    $phpErrors[] = [
        'level' => $levels[$severity] ?? 'ERROR',
        'message' => $message,
        'file' => basename($file),
        'line' => $line,
    ];
    return true;
});

// ── Config ───────────────────────────────────────
define('DATA_DIR', __DIR__ . '/data');

// ── Boot ICE ─────────────────────────────────────
require __DIR__ . '/ice/autoload.php';

use Ice\Core\Event;
use Ice\Core\StreamLog;
use Ice\Persistence\FileStore;
use Ice\Persistence\FileRefs;
use Ice\Resolution\Runner;
use function Ice\Gates\Database\registerDatabaseGates;
use function Ice\Gates\Query\SQL\registerSQLGates;

if (!is_dir(DATA_DIR)) mkdir(DATA_DIR, 0755, true);

$store  = new FileStore(DATA_DIR);
$refs   = new FileRefs(DATA_DIR);
$log    = new StreamLog('DATA');
$runner = new Runner($store, $refs, $log);
registerDatabaseGates($runner);
registerSQLGates($runner);

function buildDebug(StreamLog $log, array &$phpErrors, float $t0): array {
    return [
        'stream' => $log->sample()['entries'],
        'php'    => $phpErrors,
        'memory' => round(memory_get_peak_usage(true) / 1024),
        'time'   => round((hrtime(true) - $t0) / 1e6, 2),
    ];
}

// ── Schema endpoint ──────────────────────────────
if ($_SERVER['REQUEST_METHOD'] === 'GET' && isset($_GET['schema'])) {
    $t0 = hrtime(true);
    $allNames = $refs->list('db/tables/');
    $tables = [];
    foreach ($allNames as $name) {
        if (preg_match('#^db/tables/([^/]+)/#', $name, $m)) {
            $tables[$m[1]] = true;
        }
    }
    $schema = [];
    foreach (array_keys($tables) as $table) {
        $hash = $refs->get("db/tables/$table/schema");
        if (!$hash) continue;
        $s = $store->get($hash);
        $schema[] = ['table' => $table, 'columns' => $s['columns']];
    }
    usort($schema, fn($a, $b) => strcmp($a['table'], $b['table']));
    echo json_encode(['schema' => $schema, 'debug' => buildDebug($log, $phpErrors, $t0)]);
    exit;
}

// ── SQL endpoint ─────────────────────────────────
if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    http_response_code(405);
    echo json_encode(['error' => 'POST required']);
    exit;
}

$body = json_decode(file_get_contents('php://input'), true);
$sql  = trim($body['sql'] ?? '');

if ($sql === '') {
    echo json_encode(['error' => 'Empty SQL']);
    exit;
}

$runner->clearPending();
$t0 = hrtime(true);

try {
    $runner->emit(new Event('sql', ['sql' => $sql]));
} catch (\Throwable $e) {
    echo json_encode([
        'error' => $e->getMessage(),
        'debug' => buildDebug($log, $phpErrors, $t0),
    ]);
    exit;
}

$elapsed = (hrtime(true) - $t0) / 1e6;
$pending = $runner->sample()['pending'];

$errors       = array_filter($pending, fn($e) => $e->type === 'error');
$queryResults = array_filter($pending, fn($e) => $e->type === 'query_result');
$lastQuery    = !empty($queryResults) ? end($queryResults) : null;

$response = [
    'elapsed' => round($elapsed, 2),
    'debug'   => buildDebug($log, $phpErrors, $t0),
];

if (!empty($errors)) {
    $msgs = array_map(fn($e) => $e->data['message'], $errors);
    $response['error'] = implode('; ', $msgs);
} elseif ($lastQuery) {
    $response['rows'] = $lastQuery->data['rows'];
} else {
    foreach ($pending as $e) {
        switch ($e->type) {
            case 'table_created':
                $response['message'] = "Table '{$e->data['table']}' created."; break 2;
            case 'table_dropped':
                $response['message'] = "Table '{$e->data['table']}' dropped."; break 2;
            case 'row_inserted':
                $n = count(array_filter($pending, fn($x) => $x->type === 'row_inserted'));
                $response['message'] = "{$n} row" . ($n !== 1 ? 's' : '') . " inserted (last id: {$e->data['id']})."; break 2;
            case 'row_updated':
                $n = count($e->data['ids'] ?? []);
                $response['message'] = "{$n} row" . ($n !== 1 ? 's' : '') . " updated."; break 2;
            case 'row_deleted':
                $n = count($e->data['ids'] ?? []);
                $response['message'] = "{$n} row" . ($n !== 1 ? 's' : '') . " deleted."; break 2;
            case 'index_created':
                $response['message'] = "Index '{$e->data['index']}' created."; break 2;
            case 'index_dropped':
                $response['message'] = "Index dropped."; break 2;
            case 'view_created':
                $response['message'] = "View '{$e->data['name']}' created."; break 2;
            case 'trigger_created':
                $response['message'] = "Trigger created."; break 2;
            case 'transaction_begun':
                $response['message'] = "Transaction started."; break 2;
            case 'transaction_committed':
                $response['message'] = "Transaction committed."; break 2;
            case 'transaction_rolled_back':
                $response['message'] = "Transaction rolled back."; break 2;
        }
    }
    if (!isset($response['message'])) $response['message'] = 'OK';
}

echo json_encode($response);
