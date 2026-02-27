<?php
namespace Ice\Gates\Database;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

function aggregateRows(array $rows, array $aggregates, ?array $groupBy = null): array {
    $groups = groupRows($rows, $groupBy);
    $result = [];

    foreach ($groups as $group) {
        $out = [];
        if ($groupBy !== null && count($groupBy) > 0 && count($group['rows']) > 0) {
            foreach ($groupBy as $col) {
                // Strip table prefix for output: s.region → region
                $outKey = str_contains($col, '.') ? substr($col, strpos($col, '.') + 1) : $col;
                $out[$outKey] = resolveCol($group['rows'][0], $col);
            }
        }
        foreach ($aggregates as $agg) {
            $alias = $agg['alias'] ?? ($agg['fn'] . '_' . $agg['column']);
            $val = computeAggregate(
                $agg['fn'],
                $agg['column'],
                $group['rows'],
                $agg['distinct'] ?? false,
                $agg['separator'] ?? ','
            );
            $out[$alias] = $val;
            // Add synthetic key for HAVING: e.g. "SUM(amount)"
            $syntheticKey = strtoupper($agg['fn']) . '(' . $agg['column'] . ')';
            if ($syntheticKey !== $alias) {
                $out[$syntheticKey] = $val;
            }
        }
        $result[] = $out;
    }
    return $result;
}

function groupRows(array $rows, ?array $groupBy): array {
    if ($groupBy === null || count($groupBy) === 0) {
        return [['key' => null, 'rows' => $rows]];
    }

    $groups = [];
    foreach ($rows as $row) {
        $key = json_encode(array_map(fn($c) => resolveCol($row, $c), $groupBy));
        if (!isset($groups[$key])) $groups[$key] = [];
        $groups[$key][] = $row;
    }

    $result = [];
    foreach ($groups as $key => $grpRows) {
        $result[] = ['key' => $key, 'rows' => $grpRows];
    }
    return $result;
}

function computeAggregate(string $fn, string $column, array $rows, bool $distinct = false, string $separator = ','): mixed {
    return match (strtoupper($fn)) {
        'COUNT' => (function () use ($column, $rows, $distinct) {
            if ($column === '*') return count($rows);
            $vals = array_filter(array_map(fn($r) => resolveCol($r, $column), $rows), fn($v) => $v !== null);
            if ($distinct) $vals = array_unique($vals);
            return count($vals);
        })(),
        'SUM' => array_sum(numericValues($rows, $column)) ?: 0,
        'AVG' => (function () use ($rows, $column) {
            $vals = numericValues($rows, $column);
            return count($vals) === 0 ? null : array_sum($vals) / count($vals);
        })(),
        'MIN' => (function () use ($rows, $column) {
            $vals = nonNullValues($rows, $column);
            return count($vals) === 0 ? null : min($vals);
        })(),
        'MAX' => (function () use ($rows, $column) {
            $vals = nonNullValues($rows, $column);
            return count($vals) === 0 ? null : max($vals);
        })(),
        'GROUP_CONCAT' => (function () use ($rows, $column, $distinct, $separator) {
            $vals = array_filter(array_map(fn($r) => resolveCol($r, $column), $rows), fn($v) => $v !== null);
            $vals = array_map('strval', $vals);
            if ($distinct) $vals = array_values(array_unique($vals));
            return count($vals) > 0 ? implode($separator, $vals) : null;
        })(),
        default => null,
    };
}

function numericValues(array $rows, string $column): array {
    return array_values(array_filter(
        array_map(fn($r) => resolveCol($r, $column), $rows),
        fn($v) => $v !== null && is_numeric($v)
    ));
}

function nonNullValues(array $rows, string $column): array {
    return array_values(array_filter(
        array_map(fn($r) => resolveCol($r, $column), $rows),
        fn($v) => $v !== null
    ));
}

class AggregateGate extends PureGate {
    public function __construct() { parent::__construct('aggregate'); }

    public function transform(Event $event): Event|array|null {
        $result = aggregateRows(
            $event->data['rows'],
            $event->data['aggregates'],
            $event->data['groupBy'] ?? null
        );
        return new Event('aggregate_result', ['rows' => $result]);
    }
}
