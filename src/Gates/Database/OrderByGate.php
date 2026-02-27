<?php
namespace Ice\Gates\Database;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

function orderByRows(array $rows, ?array $order): array {
    if ($order === null || count($order) === 0) return $rows;

    usort($rows, function ($a, $b) use ($order) {
        foreach ($order as $spec) {
            $col = $spec['column'];
            $dir = strtolower($spec['direction'] ?? 'asc') === 'desc' ? -1 : 1;
            $nullsFirst = isset($spec['nulls'])
                ? ($spec['nulls'] === 'first')
                : ($dir === -1); // Default: NULLS LAST for ASC, NULLS FIRST for... no, standard is NULLS LAST for ASC, NULLS FIRST for DESC
            // Actually standard SQL: NULLS LAST for ASC, NULLS FIRST for DESC — but we'll default NULLS LAST always, override with spec
            $nullsFirst = isset($spec['nulls']) ? ($spec['nulls'] === 'first') : false;

            $va = resolveCol($a, $col);
            $vb = resolveCol($b, $col);

            $aNull = ($va === null);
            $bNull = ($vb === null);
            if ($aNull && $bNull) continue;
            if ($aNull) return $nullsFirst ? -1 : 1;
            if ($bNull) return $nullsFirst ? 1 : -1;

            if ($va < $vb) return -1 * $dir;
            if ($va > $vb) return 1 * $dir;
        }
        return 0;
    });
    return $rows;
}

class OrderByGate extends PureGate {
    public function __construct() { parent::__construct('order_by'); }

    public function transform(Event $event): Event|array|null {
        $sorted = orderByRows($event->data['rows'], $event->data['order'] ?? null);
        return new Event('ordered_result', ['rows' => $sorted]);
    }
}
