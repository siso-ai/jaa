<?php
namespace Ice\Gates\Database;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

function limitRows(array $rows, ?int $limit, int $offset = 0): array {
    if ($limit === null) return array_slice($rows, $offset);
    return array_slice($rows, $offset, $limit);
}

class LimitGate extends PureGate {
    public function __construct() { parent::__construct('limit'); }

    public function transform(Event $event): Event|array|null {
        $limited = limitRows($event->data['rows'], $event->data['limit'] ?? null, $event->data['offset'] ?? 0);
        return new Event('limited_result', ['rows' => $limited]);
    }
}
