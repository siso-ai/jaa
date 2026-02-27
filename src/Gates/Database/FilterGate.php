<?php
namespace Ice\Gates\Database;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

function filterRows(array $rows, ?array $where): array {
    if ($where === null) return $rows;
    return array_values(array_filter($rows, fn($row) => evaluateCondition($where, $row)));
}

class FilterGate extends PureGate {
    public function __construct() { parent::__construct('filter'); }

    public function transform(Event $event): Event|array|null {
        $filtered = filterRows($event->data['rows'], $event->data['where'] ?? null);
        return new Event('filter_result', ['rows' => $filtered]);
    }
}
