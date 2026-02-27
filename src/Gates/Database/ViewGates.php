<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class ViewCreateExecuteGate extends StateGate {
    public function __construct() { parent::__construct('view_create_execute'); }

    public function reads(Event $event): ReadSet {
        return (new ReadSet())->ref("db/views/{$event->data['name']}");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $name = $event->data['name'];
        if ($state['refs']["db/views/$name"] !== null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "View '$name' already exists", 'source' => 'view_create_execute']));
        }
        return (new MutationBatch())
            ->put('view', ['name' => $name, 'query' => $event->data['query'], 'columns' => $event->data['columns'] ?? null])
            ->refSet("db/views/$name", 0)
            ->emit(new Event('view_created', ['name' => $name]));
    }
}

class ViewDropExecuteGate extends StateGate {
    public function __construct() { parent::__construct('view_drop_execute'); }

    public function reads(Event $event): ReadSet {
        return (new ReadSet())->ref("db/views/{$event->data['name']}");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $name = $event->data['name'];
        if ($state['refs']["db/views/$name"] === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "View '$name' does not exist", 'source' => 'view_drop_execute']));
        }
        return (new MutationBatch())
            ->refDelete("db/views/$name")
            ->emit(new Event('view_dropped', ['name' => $name]));
    }
}

class ViewExpansionGate extends StateGate {
    public function __construct() { parent::__construct('view_expand'); }

    public function reads(Event $event): ReadSet {
        return (new ReadSet())->ref("db/views/{$event->data['view']}");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $viewDef = $state['refs']["db/views/{$event->data['view']}"];
        if ($viewDef === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "View '{$event->data['view']}' does not exist", 'source' => 'view_expand']));
        }
        return (new MutationBatch())
            ->emit(new Event('query_plan', ['pipeline' => $viewDef['query']['pipeline'] ?? []]));
    }
}
