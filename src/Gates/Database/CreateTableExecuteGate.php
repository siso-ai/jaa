<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class CreateTableExecuteGate extends StateGate {
    public function __construct() { parent::__construct('create_table_execute'); }

    public function reads(Event $event): ReadSet {
        return (new ReadSet())->ref("db/tables/{$event->data['table']}/schema");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $columns = $event->data['columns'] ?? [];
        $existing = $state['refs']["db/tables/$table/schema"];

        if ($existing !== null) {
            if (!empty($event->data['ifNotExists'])) {
                return (new MutationBatch())
                    ->emit(new Event('table_exists', ['table' => $table]));
            }
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Table '$table' already exists", 'source' => 'create_table_execute']));
        }

        return (new MutationBatch())
            ->put('schema', ['name' => $table, 'columns' => $columns])
            ->refSet("db/tables/$table/schema", 0)
            ->put('counter', '0')
            ->refSet("db/tables/$table/next_id", 1)
            ->emit(new Event('table_created', ['table' => $table]));
    }
}
