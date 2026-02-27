<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class InsertExecuteGate extends StateGate {
    public function __construct() { parent::__construct('insert_execute'); }

    public function reads(Event $event): ReadSet {
        $t = $event->data['table'];
        $rs = (new ReadSet())
            ->ref("db/tables/$t/schema")
            ->ref("db/tables/$t/next_id")
            ->pattern("db/tables/$t/indexes/");
        // Need existing rows for conflict check
        if (isset($event->data['onConflict'])) {
            $rs->pattern("db/tables/$t/rows/");
        }
        return $rs;
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $row = $event->data['row'];
        $schema = $state['refs']["db/tables/$table/schema"];
        $onConflict = $event->data['onConflict'] ?? null;

        if ($schema === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Table '$table' does not exist", 'source' => 'insert_execute']));
        }

        // UPSERT: check for conflict
        if ($onConflict && $onConflict['column']) {
            $conflictCol = $onConflict['column'];
            $conflictVal = $row[$conflictCol] ?? null;
            $allRows = $state['patterns']["db/tables/$table/rows/"] ?? [];

            foreach ($allRows as $refName => $existingRow) {
                if (($existingRow[$conflictCol] ?? null) === $conflictVal) {
                    // Conflict found
                    if ($onConflict['action'] === 'nothing') {
                        $batch = (new MutationBatch())
                            ->emit(new Event('row_inserted', ['table' => $table, 'id' => $existingRow['id'], 'row' => $existingRow, 'conflict' => 'skipped']));
                        if ($event->data['returning'] ?? null) {
                            $batch->emit(new Event('query_result', ['rows' => [self::applyReturning($existingRow, $event->data['returning'])]]));
                        }
                        return $batch;
                    }
                    // DO UPDATE SET
                    $newRow = $existingRow;
                    foreach ($onConflict['updates'] as $col => $expr) {
                        $newRow[$col] = evaluateExpression($expr, $existingRow);
                    }
                    $batch = (new MutationBatch())
                        ->put('row', $newRow)
                        ->refSet($refName, 0);

                    // Rebuild indexes
                    $indexes = $state['patterns']["db/tables/$table/indexes/"] ?? [];
                    $putIdx = 1;
                    foreach ($indexes as $idxRef => $index) {
                        $remaining = [];
                        foreach ($allRows as $rn => $r) {
                            $remaining[] = ($rn === $refName) ? $newRow : $r;
                        }
                        $rebuilt = rebuildIndex($index, $remaining);
                        $batch->put('btree', $rebuilt);
                        $batch->refSet($idxRef, $putIdx++);
                    }

                    $batch->emit(new Event('row_inserted', ['table' => $table, 'id' => $newRow['id'], 'row' => $newRow, 'conflict' => 'updated']));
                    if ($event->data['returning'] ?? null) {
                        $batch->emit(new Event('query_result', ['rows' => [self::applyReturning($newRow, $event->data['returning'])]]));
                    }
                    return $batch;
                }
            }
            // No conflict → proceed with normal insert
        }

        $counter = intval($state['refs']["db/tables/$table/next_id"] ?? '0');
        $id = $counter + 1;

        $completeRow = ['id' => $id];
        foreach ($schema['columns'] as $col) {
            if ($col['name'] === 'id') continue;
            if (array_key_exists($col['name'], $row)) {
                $completeRow[$col['name']] = $row[$col['name']];
            } elseif (isset($col['default']) && $col['default'] !== null) {
                $completeRow[$col['name']] = $col['default'];
            } elseif (isset($col['nullable']) && $col['nullable'] === false) {
                return (new MutationBatch())
                    ->emit(new Event('error', ['message' => "Column '{$col['name']}' cannot be null", 'source' => 'insert_execute']));
            } else {
                $completeRow[$col['name']] = null;
            }
        }

        $batch = (new MutationBatch())
            ->put('row', $completeRow)
            ->refSet("db/tables/$table/rows/$id", 0)
            ->put('counter', (string)$id)
            ->refSet("db/tables/$table/next_id", 1);

        $indexes = $state['patterns']["db/tables/$table/indexes/"] ?? [];
        $putIdx = 2;
        foreach ($indexes as $refName => $index) {
            $updated = addToIndex($index, $completeRow);
            $batch->put('btree', $updated);
            $batch->refSet($refName, $putIdx++);
        }

        $batch->emit(new Event('row_inserted', ['table' => $table, 'id' => $id, 'row' => $completeRow]));

        // RETURNING
        $returning = $event->data['returning'] ?? null;
        if ($returning) {
            $batch->emit(new Event('query_result', ['rows' => [self::applyReturning($completeRow, $returning)]]));
        }

        return $batch;
    }

    private static function applyReturning(array $row, array $cols): array {
        if ($cols === ['*']) return $row;
        $out = [];
        foreach ($cols as $c) {
            $out[$c] = $row[$c] ?? null;
        }
        return $out;
    }
}

function addToIndex(array $index, array $row): array {
    $entries = $index['entries'] ?? [];
    $key = $row[$index['column']] ?? null;
    $found = false;
    foreach ($entries as &$entry) {
        if ($entry['key'] === $key) {
            $entry['row_ids'][] = $row['id'];
            $found = true;
            break;
        }
    }
    unset($entry);
    if (!$found) {
        $entries[] = ['key' => $key, 'row_ids' => [$row['id']]];
    }
    return array_merge($index, ['entries' => $entries]);
}
