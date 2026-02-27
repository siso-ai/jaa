<?php
namespace Ice\Gates\Database;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

function distinctRows(array $rows, ?array $columns = null): array {
    $seen = [];
    $result = [];
    foreach ($rows as $row) {
        $key = ($columns !== null && count($columns) > 0)
            ? json_encode(array_map(fn($c) => $row[$c] ?? null, $columns))
            : json_encode($row);
        if (!isset($seen[$key])) {
            $seen[$key] = true;
            $result[] = $row;
        }
    }
    return $result;
}

class DistinctGate extends PureGate {
    public function __construct() { parent::__construct('distinct'); }

    public function transform(Event $event): Event|array|null {
        $unique = distinctRows($event->data['rows'], $event->data['columns'] ?? null);
        return new Event('distinct_result', ['rows' => $unique]);
    }
}
