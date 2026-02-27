<?php
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class ViewCreateParseGate extends PureGate {
    public function __construct() { parent::__construct('view_create_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 2; // skip CREATE VIEW

        $name = $tokens[$pos]['value'];
        $pos++;

        $pos++; // skip AS

        // Grab remaining tokens as the sub-select
        $subTokens = array_slice($tokens, $pos);
        $asPos = stripos($event->data['sql'], ' AS ');
        $subSql = trim(substr($event->data['sql'], $asPos + 4));

        return new Event('view_create_execute', [
            'name' => $name,
            'query' => ['sql' => $subSql, 'tokens' => $subTokens],
            'columns' => null,
        ]);
    }
}

class ViewDropParseGate extends PureGate {
    public function __construct() { parent::__construct('view_drop_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        return new Event('view_drop_execute', ['name' => $tokens[2]['value']]);
    }
}
