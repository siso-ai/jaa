<?php
/**
 * InsertSelectPlanGate — executes INSERT...SELECT and CREATE TABLE AS SELECT.
 * StateGate: reads from source tables, inserts into target.
 */
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

use function Ice\Gates\Database\filterRows;
use function Ice\Gates\Database\projectRows;
use function Ice\Gates\Database\orderByRows;
use function Ice\Gates\Database\limitRows;
use function Ice\Gates\Database\distinctRows;
use function Ice\Gates\Database\aggregateRows;
use function Ice\Gates\Database\joinRows;

class InsertSelectPlanGate extends StateGate {
    public function __construct() { parent::__construct('insert_select_plan'); }

    public function reads(Event $event): ReadSet {
        $rs = new ReadSet();
        $table = $event->data['table'];
        $rs->ref("db/tables/$table/schema");
        $rs->ref("db/tables/$table/next_id");
        self::scanPipelineReads($event->data['pipeline'], $rs);
        return $rs;
    }

    private static function scanPipelineReads(array $pipeline, ReadSet $rs): void {
        foreach ($pipeline as $step) {
            if ($step['type'] === 'table_scan') $rs->pattern("db/tables/{$step['data']['table']}/rows/");
            if ($step['type'] === 'index_scan') {
                $rs->ref("db/tables/{$step['data']['table']}/indexes/{$step['data']['index']}");
                $rs->pattern("db/tables/{$step['data']['table']}/rows/");
            }
            if ($step['type'] === 'join') $rs->pattern("db/tables/{$step['data']['right']['table']}/rows/");
            if ($step['type'] === 'union') {
                self::scanPipelineReads($step['data']['left'], $rs);
                self::scanPipelineReads($step['data']['right'], $rs);
            }
        }
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $columns = $event->data['columns'] ?? null;
        $pipeline = $event->data['pipeline'];
        $createTable = $event->data['createTable'] ?? false;
        $ifNotExists = $event->data['ifNotExists'] ?? false;

        // Execute the SELECT pipeline to get source rows
        $sourceRows = self::executePipeline($pipeline, $state);
        if (count($sourceRows) === 0) {
            if ($createTable) {
                // Still create an empty table
                return (new MutationBatch())
                    ->put('schema', ['name' => $table, 'columns' => []])
                    ->refSet("db/tables/$table/schema", 0)
                    ->put('counter', '0')
                    ->refSet("db/tables/$table/next_id", 1)
                    ->emit(new Event('table_created', ['table' => $table]));
            }
            return (new MutationBatch())->emit(new Event('rows_inserted', ['table' => $table, 'count' => 0]));
        }

        $batch = new MutationBatch();
        $putIdx = 0;

        // CREATE TABLE AS SELECT: infer schema from first row
        if ($createTable) {
            $existing = $state['refs']["db/tables/$table/schema"] ?? null;
            if ($existing !== null) {
                if ($ifNotExists) {
                    return (new MutationBatch())->emit(new Event('table_exists', ['table' => $table]));
                }
                return (new MutationBatch())
                    ->emit(new Event('error', ['message' => "Table '$table' already exists", 'source' => 'insert_select_plan']));
            }

            // Infer columns from source rows (exclude 'id')
            $colNames = array_keys($sourceRows[0]);
            $colNames = array_filter($colNames, fn($c) => $c !== 'id');
            $schemaCols = array_map(fn($c) => ['name' => $c, 'type' => 'text', 'nullable' => true, 'default' => null], array_values($colNames));

            $batch->put('schema', ['name' => $table, 'columns' => $schemaCols]);
            $batch->refSet("db/tables/$table/schema", $putIdx++);
        } else {
            // Check table exists
            $schema = $state['refs']["db/tables/$table/schema"] ?? null;
            if ($schema === null) {
                return (new MutationBatch())
                    ->emit(new Event('error', ['message' => "Table '$table' does not exist", 'source' => 'insert_select_plan']));
            }
        }

        // Determine starting ID
        $counter = (int)($state['refs']["db/tables/$table/next_id"] ?? '0');

        // Insert rows
        foreach ($sourceRows as $srcRow) {
            $counter++;
            $newRow = ['id' => $counter];

            if ($columns !== null) {
                // Map by column list
                $srcValues = array_values($srcRow);
                foreach ($columns as $i => $col) {
                    $newRow[$col] = $srcValues[$i] ?? null;
                }
            } else {
                // Copy all non-id columns
                foreach ($srcRow as $k => $v) {
                    if ($k !== 'id') $newRow[$k] = $v;
                }
            }

            $batch->put('row', $newRow);
            $batch->refSet("db/tables/$table/rows/$counter", $putIdx++);
        }

        $batch->put('counter', (string)$counter);
        $batch->refSet("db/tables/$table/next_id", $putIdx++);

        return $batch->emit(new Event('rows_inserted', ['table' => $table, 'count' => count($sourceRows)]));
    }

    /** Execute a pipeline against state — same as QueryPlanGate but reused here */
    private static function executePipeline(array $pipeline, array $state): array {
        $rows = [];
        foreach ($pipeline as $step) {
            switch ($step['type']) {
                case 'table_scan':
                    $rows = array_values($state['patterns']["db/tables/{$step['data']['table']}/rows/"] ?? []);
                    break;
                case 'filter':
                    $rows = filterRows($rows, $step['data']['where']);
                    break;
                case 'project':
                    $rows = projectRows($rows, $step['data']['columns']);
                    break;
                case 'order_by':
                    $rows = orderByRows($rows, $step['data']['order']);
                    break;
                case 'limit':
                    $rows = limitRows($rows, $step['data']['limit'], $step['data']['offset'] ?? 0);
                    break;
                case 'distinct':
                    $rows = distinctRows($rows, $step['data']['columns'] ?? null);
                    break;
                case 'aggregate':
                    $rows = aggregateRows($rows, $step['data']['aggregates'], $step['data']['groupBy'] ?? null);
                    break;
                case 'join':
                    $rightRows = array_values($state['patterns']["db/tables/{$step['data']['right']['table']}/rows/"] ?? []);
                    $rows = joinRows($rows, $rightRows, $step['data']['on'], $step['data']['type'] ?? 'inner');
                    break;
                case 'union':
                    $leftRows = self::executePipeline($step['data']['left'], $state);
                    $rightRows = self::executePipeline($step['data']['right'], $state);
                    $rows = array_merge($leftRows, $rightRows);
                    if (!($step['data']['all'] ?? false)) $rows = distinctRows($rows, null);
                    break;
            }
        }
        return $rows;
    }
}
