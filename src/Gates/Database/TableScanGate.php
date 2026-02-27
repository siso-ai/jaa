<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class TableScanGate extends StateGate {
    public function __construct() { parent::__construct('table_scan'); }

    public function reads(Event $event): ReadSet {
        return (new ReadSet())->pattern("db/tables/{$event->data['table']}/rows/");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $rows = array_values($state['patterns']["db/tables/{$event->data['table']}/rows/"] ?? []);
        return (new MutationBatch())
            ->emit(new Event('scan_result', ['table' => $event->data['table'], 'rows' => $rows]));
    }
}
