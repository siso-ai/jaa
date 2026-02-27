<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class ConstraintCreateExecuteGate extends StateGate {
    public function __construct() { parent::__construct('constraint_create_execute'); }

    public function reads(Event $event): ReadSet {
        $table = $event->data['table'];
        $name = $event->data['name'];
        return (new ReadSet())
            ->ref("db/tables/$table/schema")
            ->ref("db/constraints/$table/$name");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $name = $event->data['name'];
        $schema = $state['refs']["db/tables/$table/schema"];
        $existing = $state['refs']["db/constraints/$table/$name"];

        if ($schema === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Table '$table' does not exist", 'source' => 'constraint_create_execute']));
        }
        if ($existing !== null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Constraint '$name' already exists", 'source' => 'constraint_create_execute']));
        }

        return (new MutationBatch())
            ->put('constraint', ['name' => $name, 'table' => $table, 'type' => $event->data['type'], 'params' => $event->data['params']])
            ->refSet("db/constraints/$table/$name", 0)
            ->emit(new Event('constraint_created', ['table' => $table, 'name' => $name]));
    }
}

class ConstraintDropExecuteGate extends StateGate {
    public function __construct() { parent::__construct('constraint_drop_execute'); }

    public function reads(Event $event): ReadSet {
        return (new ReadSet())
            ->ref("db/constraints/{$event->data['table']}/{$event->data['name']}");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $name = $event->data['name'];
        if ($state['refs']["db/constraints/$table/$name"] === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Constraint '$name' does not exist", 'source' => 'constraint_drop_execute']));
        }
        return (new MutationBatch())
            ->refDelete("db/constraints/$table/$name")
            ->emit(new Event('constraint_dropped', ['table' => $table, 'name' => $name]));
    }
}
