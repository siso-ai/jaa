<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class TriggerCreateExecuteGate extends StateGate {
    public function __construct() { parent::__construct('trigger_create_execute'); }

    public function reads(Event $event): ReadSet {
        return (new ReadSet())->ref("db/triggers/{$event->data['name']}");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $name = $event->data['name'];
        if ($state['refs']["db/triggers/$name"] !== null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Trigger '$name' already exists", 'source' => 'trigger_create_execute']));
        }
        return (new MutationBatch())
            ->put('trigger', [
                'name' => $name,
                'table' => $event->data['table'],
                'timing' => $event->data['timing'],
                'event' => $event->data['event'],
                'action' => $event->data['action'],
            ])
            ->refSet("db/triggers/$name", 0)
            ->emit(new Event('trigger_created', ['name' => $name]));
    }
}

class TriggerDropExecuteGate extends StateGate {
    public function __construct() { parent::__construct('trigger_drop_execute'); }

    public function reads(Event $event): ReadSet {
        return (new ReadSet())->ref("db/triggers/{$event->data['name']}");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $name = $event->data['name'];
        if ($state['refs']["db/triggers/$name"] === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Trigger '$name' does not exist", 'source' => 'trigger_drop_execute']));
        }
        return (new MutationBatch())
            ->refDelete("db/triggers/$name")
            ->emit(new Event('trigger_dropped', ['name' => $name]));
    }
}
