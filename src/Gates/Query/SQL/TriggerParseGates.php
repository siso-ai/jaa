<?php
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class TriggerCreateParseGate extends PureGate {
    public function __construct() { parent::__construct('trigger_create_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 2; // skip CREATE TRIGGER

        $name = $tokens[$pos]['value'];
        $pos++;

        // BEFORE|AFTER
        $timing = strtolower($tokens[$pos]['value']);
        $pos++;

        // INSERT|UPDATE|DELETE
        $triggerEvent = strtolower($tokens[$pos]['value']);
        $pos++;

        // ON table
        $pos++; // ON
        $table = $tokens[$pos]['value'];
        $pos++;

        // Rest is the action body
        $action = ['tokens' => array_slice($tokens, $pos)];

        return new Event('trigger_create_execute', [
            'name' => $name, 'table' => $table, 'timing' => $timing,
            'event' => $triggerEvent, 'action' => $action,
        ]);
    }
}

class TriggerDropParseGate extends PureGate {
    public function __construct() { parent::__construct('trigger_drop_parse'); }

    public function transform(Event $event): Event|array|null {
        return new Event('trigger_drop_execute', ['name' => $event->data['tokens'][2]['value']]);
    }
}
