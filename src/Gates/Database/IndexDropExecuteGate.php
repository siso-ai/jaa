<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class IndexDropExecuteGate extends StateGate {
    public function __construct() { parent::__construct('index_drop_execute'); }

    public function reads(Event $event): ReadSet {
        return (new ReadSet())
            ->ref("db/tables/{$event->data['table']}/indexes/{$event->data['index']}");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $index = $event->data['index'];

        if ($state['refs']["db/tables/$table/indexes/$index"] === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Index '$index' does not exist", 'source' => 'index_drop_execute']));
        }

        return (new MutationBatch())
            ->refDelete("db/tables/$table/indexes/$index")
            ->emit(new Event('index_dropped', ['table' => $table, 'index' => $index]));
    }
}
