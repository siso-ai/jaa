<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

function joinRows(array $leftRows, array $rightRows, array|null $on, string $type = 'inner', string $leftTable = '', string $rightTable = ''): array {
    $results = [];
    $rightMatched = [];

    // CROSS JOIN — cartesian product
    if ($type === 'cross' || ($on === null && $type === 'inner')) {
        foreach ($leftRows as $left) {
            foreach ($rightRows as $right) {
                $results[] = mergeJoinRow($left, $right, $leftTable, $rightTable);
            }
        }
        return $results;
    }

    foreach ($leftRows as $left) {
        $matched = false;
        foreach ($rightRows as $i => $right) {
            if (matchesJoin($left, $right, $on)) {
                $results[] = mergeJoinRow($left, $right, $leftTable, $rightTable);
                $rightMatched[$i] = true;
                $matched = true;
            }
        }
        if (!$matched && ($type === 'left' || $type === 'full')) {
            $results[] = mergeJoinRow($left, nullRow($rightRows), $leftTable, $rightTable);
        }
    }

    if ($type === 'right' || $type === 'full') {
        foreach ($rightRows as $i => $right) {
            if (!isset($rightMatched[$i])) {
                $results[] = mergeJoinRow(nullRow($leftRows), $right, $leftTable, $rightTable);
            }
        }
    }

    return $results;
}

/** Merge left and right rows, handling column name conflicts */
function mergeJoinRow(array $left, array $right, string $leftTable, string $rightTable): array {
    $merged = [];
    // Add all left columns with qualified names
    foreach ($left as $key => $value) {
        $merged[$key] = $value;
        if ($leftTable) $merged["$leftTable.$key"] = $value;
    }
    // Add right columns with qualified names; on conflict, don't overwrite base key
    foreach ($right as $key => $value) {
        if ($rightTable) $merged["$rightTable.$key"] = $value;
        if (!array_key_exists($key, $merged)) {
            $merged[$key] = $value;
        }
    }
    return $merged;
}

function matchesJoin(array $left, array $right, array|null $on): bool {
    if ($on === null) return false;
    if (isset($on['left'])) {
        return resolveCol($left, $on['left']) === resolveCol($right, $on['right']);
    }
    foreach ($on as $o) {
        if (resolveCol($left, $o['left']) !== resolveCol($right, $o['right'])) return false;
    }
    return true;
}

function resolveCol(array $row, string $col): mixed {
    if (array_key_exists($col, $row)) return $row[$col];
    // alias.column fallback
    $dot = strpos($col, '.');
    if ($dot !== false) {
        $short = substr($col, $dot + 1);
        if (array_key_exists($short, $row)) return $row[$short];
    }
    return null;
}

function nullRow(array $sampleRows): array {
    if (count($sampleRows) === 0) return [];
    $row = [];
    foreach (array_keys($sampleRows[0]) as $key) {
        $row[$key] = null;
    }
    return $row;
}

class JoinGate extends StateGate {
    public function __construct() { parent::__construct('join'); }

    public function reads(Event $event): ReadSet {
        return (new ReadSet())
            ->pattern("db/tables/{$event->data['left']['table']}/rows/")
            ->pattern("db/tables/{$event->data['right']['table']}/rows/");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $left = $event->data['left'];
        $right = $event->data['right'];
        $on = $event->data['on'];
        $type = $event->data['type'] ?? 'inner';

        $leftRows = array_values($state['patterns']["db/tables/{$left['table']}/rows/"] ?? []);
        $rightRows = array_values($state['patterns']["db/tables/{$right['table']}/rows/"] ?? []);
        $rows = joinRows($leftRows, $rightRows, $on, $type);

        return (new MutationBatch())
            ->emit(new Event('join_result', ['rows' => $rows]));
    }
}
