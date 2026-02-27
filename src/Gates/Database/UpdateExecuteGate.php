<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class UpdateExecuteGate extends StateGate {
    public function __construct() { parent::__construct('update_execute'); }

    public function reads(Event $event): ReadSet {
        $t = $event->data['table'];
        $rs = (new ReadSet())
            ->ref("db/tables/$t/schema")
            ->pattern("db/tables/$t/rows/")
            ->pattern("db/tables/$t/indexes/");
        // FROM table for join updates
        if (isset($event->data['fromTable'])) {
            $ft = $event->data['fromTable'];
            $rs->pattern("db/tables/$ft/rows/");
        }
        // Scan WHERE for subquery table references
        if (isset($event->data['where'])) {
            \Ice\Gates\Query\SQL\QueryPlanGate::scanConditionSubqueries($event->data['where'], $rs);
        }
        return $rs;
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $changes = $event->data['changes'];
        $where = $event->data['where'] ?? null;
        // Resolve subqueries in WHERE
        if ($where !== null) {
            $where = \Ice\Gates\Query\SQL\QueryPlanGate::resolveConditionSubqueries($where, $state);
        }
        $id = $event->data['id'] ?? null;
        $schema = $state['refs']["db/tables/$table/schema"];
        $fromTable = $event->data['fromTable'] ?? null;
        $fromAlias = $event->data['fromAlias'] ?? $fromTable;

        if ($schema === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Table '$table' does not exist", 'source' => 'update_execute']));
        }

        $allRows = $state['patterns']["db/tables/$table/rows/"] ?? [];
        $targets = [];

        if ($fromTable !== null) {
            // JOIN UPDATE: merge target rows with FROM table rows
            $fromRows = array_values($state['patterns']["db/tables/$fromTable/rows/"] ?? []);
            foreach ($allRows as $refName => $row) {
                foreach ($fromRows as $fromRow) {
                    // Create merged context with qualified names
                    $merged = $row;
                    foreach ($fromRow as $k => $v) {
                        $merged["$fromAlias.$k"] = $v;
                        $merged[$fromTable . ".$k"] = $v;
                        if (!array_key_exists($k, $merged) || $k === 'id') {
                            // Don't override target columns, but add from-table columns
                        } else {
                            // Conflict: keep both under qualified names
                            $merged["$table.$k"] = $row[$k];
                        }
                        $merged[$k] = $merged[$k] ?? $v; // FROM col fills in if not in target
                    }
                    // Also add target qualified names
                    foreach ($row as $k => $v) {
                        $merged["$table.$k"] = $v;
                    }
                    if ($where === null || evaluateCondition($where, $merged)) {
                        $targets[] = ['refName' => $refName, 'row' => $row, 'context' => $merged];
                    }
                }
            }
        } else {
            foreach ($allRows as $refName => $row) {
                if ($id !== null && ($row['id'] ?? null) === $id) {
                    $targets[] = ['refName' => $refName, 'row' => $row, 'context' => $row];
                } elseif ($where !== null && evaluateCondition($where, $row)) {
                    $targets[] = ['refName' => $refName, 'row' => $row, 'context' => $row];
                } elseif ($id === null && $where === null) {
                    $targets[] = ['refName' => $refName, 'row' => $row, 'context' => $row];
                }
            }
        }

        if (count($targets) === 0) {
            return (new MutationBatch())
                ->emit(new Event('row_updated', ['table' => $table, 'ids' => [], 'changes' => $changes]));
        }

        $batch = new MutationBatch();
        $updatedIds = [];
        $updatedRows = [];
        $putIdx = 0;
        $changesExprs = $event->data['changesExprs'] ?? [];

        foreach ($targets as $t) {
            // Compute expression-based changes for this row (use context for FROM table access)
            $ctx = $t['context'] ?? $t['row'];
            $rowChanges = $changes;
            foreach ($changesExprs as $col => $expr) {
                $rowChanges[$col] = evaluateExpression($expr, $ctx);
            }
            $newRow = array_merge($t['row'], $rowChanges);
            $batch->put('row', $newRow);
            $batch->refSet($t['refName'], $putIdx++);
            $updatedIds[] = $t['row']['id'];
            $updatedRows[] = $newRow;
        }

        // Rebuild indexes
        $indexes = $state['patterns']["db/tables/$table/indexes/"] ?? [];
        $targetRefs = array_map(fn($t) => $t['refName'], $targets);
        $targetMap = [];
        foreach ($targets as $i => $t) {
            $ctx = $t['context'] ?? $t['row'];
            $rowChanges = $changes;
            foreach ($changesExprs as $col => $expr) {
                $rowChanges[$col] = evaluateExpression($expr, $ctx);
            }
            $targetMap[$t['refName']] = $rowChanges;
        }
        foreach ($indexes as $idxRef => $index) {
            $allUpdated = [];
            foreach ($allRows as $refName => $row) {
                if (isset($targetMap[$refName])) {
                    $allUpdated[] = array_merge($row, $targetMap[$refName]);
                } else {
                    $allUpdated[] = $row;
                }
            }
            $rebuilt = rebuildIndex($index, $allUpdated);
            $batch->put('btree', $rebuilt);
            $batch->refSet($idxRef, $putIdx++);
        }

        $batch->emit(new Event('row_updated', ['table' => $table, 'ids' => $updatedIds, 'changes' => $changes]));

        // RETURNING
        $returning = $event->data['returning'] ?? null;
        if ($returning && count($updatedRows) > 0) {
            $returnedRows = array_map(function ($row) use ($returning) {
                if ($returning === ['*']) return $row;
                $out = [];
                foreach ($returning as $c) $out[$c] = $row[$c] ?? null;
                return $out;
            }, $updatedRows);
            $batch->emit(new Event('query_result', ['rows' => $returnedRows]));
        }

        return $batch;
    }
}

function rebuildIndex(array $index, array $rows): array {
    $entries = [];
    foreach ($rows as $row) {
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
    }
    return array_merge($index, ['entries' => $entries]);
}
