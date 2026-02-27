<?php
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class IndexCreateParseGate extends PureGate {
    public function __construct() { parent::__construct('index_create_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 1; // skip CREATE

        $unique = false;
        if (kw($tokens, $pos, 'UNIQUE')) { $unique = true; $pos++; }
        $pos++; // skip INDEX

        $index = $tokens[$pos]['value'];
        $pos++;

        $pos++; // skip ON
        $table = $tokens[$pos]['value'];
        $pos++;

        $pos++; // skip (
        $column = $tokens[$pos]['value'];
        $pos++;

        return new Event('index_create_execute', [
            'table' => $table, 'index' => $index, 'column' => $column, 'unique' => $unique,
        ]);
    }
}

class IndexDropParseGate extends PureGate {
    public function __construct() { parent::__construct('index_drop_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 2; // skip DROP INDEX
        $index = $tokens[$pos]['value'];
        $pos++;

        $pos++; // skip ON
        $table = $tokens[$pos]['value'];

        return new Event('index_drop_execute', ['table' => $table, 'index' => $index]);
    }
}
