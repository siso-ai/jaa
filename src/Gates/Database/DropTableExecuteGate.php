<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class DropTableExecuteGate extends StateGate {
    public function __construct() { parent::__construct('drop_table_execute'); }

    public function reads(Event $event): ReadSet {
        $t = $event->data['table'];
        return (new ReadSet())
            ->ref("db/tables/$t/schema")
            ->pattern("db/tables/$t/rows/")
            ->pattern("db/tables/$t/indexes/");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $ifExists = $event->data['ifExists'] ?? false;
        $schema = $state['refs']["db/tables/$table/schema"];

        if ($schema === null) {
            if ($ifExists) return new MutationBatch();
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Table '$table' does not exist", 'source' => 'drop_table_execute']));
        }

        $batch = (new MutationBatch())
            ->refDelete("db/tables/$table/schema")
            ->refDelete("db/tables/$table/next_id");

        foreach (array_keys($state['patterns']["db/tables/$table/rows/"] ?? []) as $name) {
            $batch->refDelete($name);
        }
        foreach (array_keys($state['patterns']["db/tables/$table/indexes/"] ?? []) as $name) {
            $batch->refDelete($name);
        }

        return $batch->emit(new Event('table_dropped', ['table' => $table]));
    }
}
