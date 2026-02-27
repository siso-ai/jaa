<?php
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class DropTableParseGate extends PureGate {
    public function __construct() { parent::__construct('drop_table_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 2; // skip DROP TABLE

        $ifExists = false;
        if (kw($tokens, $pos, 'IF')) {
            $ifExists = true;
            $pos += 2; // skip IF EXISTS
        }

        $table = $tokens[$pos]['value'];
        return new Event('drop_table_execute', ['table' => $table, 'ifExists' => $ifExists]);
    }
}
