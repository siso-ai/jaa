<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class DeleteExecuteGate extends StateGate {
    public function __construct() { parent::__construct('delete_execute'); }

    public function reads(Event $event): ReadSet {
        $t = $event->data['table'];
        $rs = (new ReadSet())
            ->pattern("db/tables/$t/rows/")
            ->pattern("db/tables/$t/indexes/");
        if (isset($event->data['where'])) {
            \Ice\Gates\Query\SQL\QueryPlanGate::scanConditionSubqueries($event->data['where'], $rs);
        }
        return $rs;
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $where = $event->data['where'] ?? null;
        if ($where !== null) {
            $where = \Ice\Gates\Query\SQL\QueryPlanGate::resolveConditionSubqueries($where, $state);
        }
        $id = $event->data['id'] ?? null;
        $allRows = $state['patterns']["db/tables/$table/rows/"] ?? [];

        $targets = [];
        foreach ($allRows as $refName => $row) {
            if ($id !== null && ($row['id'] ?? null) === $id) {
                $targets[] = ['refName' => $refName, 'row' => $row];
            } elseif ($where !== null && evaluateCondition($where, $row)) {
                $targets[] = ['refName' => $refName, 'row' => $row];
            } elseif ($id === null && $where === null) {
                $targets[] = ['refName' => $refName, 'row' => $row];
            }
        }

        $batch = new MutationBatch();
        $deletedIds = [];

        foreach ($targets as $t) {
            $batch->refDelete($t['refName']);
            $deletedIds[] = $t['row']['id'];
        }

        // Rebuild indexes without deleted rows
        $indexes = $state['patterns']["db/tables/$table/indexes/"] ?? [];
        $targetRefs = array_map(fn($t) => $t['refName'], $targets);
        $putIdx = 0;

        foreach ($indexes as $idxRef => $index) {
            $remaining = [];
            foreach ($allRows as $refName => $row) {
                if (!in_array($refName, $targetRefs)) {
                    $remaining[] = $row;
                }
            }
            $rebuilt = rebuildIndex($index, $remaining);
            $batch->put('btree', $rebuilt);
            $batch->refSet($idxRef, $putIdx++);
        }

        $returning = $event->data['returning'] ?? null;
        $batch->emit(new Event('row_deleted', ['table' => $table, 'ids' => $deletedIds]));

        if ($returning && count($targets) > 0) {
            $returnedRows = array_map(function ($t) use ($returning) {
                $row = $t['row'];
                if ($returning === ['*']) return $row;
                $out = [];
                foreach ($returning as $c) $out[$c] = $row[$c] ?? null;
                return $out;
            }, $targets);
            $batch->emit(new Event('query_result', ['rows' => $returnedRows]));
        }

        return $batch;
    }
}
