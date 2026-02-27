<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class IndexScanGate extends StateGate {
    public function __construct() { parent::__construct('index_scan'); }

    public function reads(Event $event): ReadSet {
        $t = $event->data['table'];
        return (new ReadSet())
            ->ref("db/tables/$t/indexes/{$event->data['index']}")
            ->pattern("db/tables/$t/rows/");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $indexName = $event->data['index'];
        $op = $event->data['op'] ?? 'eq';
        $value = $event->data['value'] ?? null;
        $low = $event->data['low'] ?? null;
        $high = $event->data['high'] ?? null;
        $index = $state['refs']["db/tables/$table/indexes/$indexName"];

        if ($index === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Index '$indexName' not found", 'source' => 'index_scan']));
        }

        $matchingIds = [];
        foreach ($index['entries'] ?? [] as $entry) {
            if (matchesOp($entry['key'], $op, $value, $low, $high)) {
                foreach ($entry['row_ids'] as $id) {
                    $matchingIds[$id] = true;
                }
            }
        }

        $allRows = $state['patterns']["db/tables/$table/rows/"] ?? [];
        $rows = array_values(array_filter(
            array_values($allRows),
            fn($r) => isset($matchingIds[$r['id']])
        ));

        return (new MutationBatch())
            ->emit(new Event('scan_result', ['table' => $table, 'rows' => $rows]));
    }
}

function matchesOp(mixed $key, string $op, mixed $value, mixed $low, mixed $high): bool {
    return match ($op) {
        'eq' => $key === $value,
        'neq' => $key !== $value,
        'gt' => $key > $value,
        'lt' => $key < $value,
        'gte' => $key >= $value,
        'lte' => $key <= $value,
        'range' => $key >= $low && $key <= $high,
        default => false,
    };
}
