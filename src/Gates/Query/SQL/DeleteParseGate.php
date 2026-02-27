<?php
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class DeleteParseGate extends PureGate {
    public function __construct() { parent::__construct('delete_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 1; // skip DELETE

        if (kw($tokens, $pos, 'FROM')) $pos++;

        $table = $tokens[$pos]['value'];
        $pos++;

        $where = null;
        if (kw($tokens, $pos, 'WHERE')) {
            $pos++;
            $result = parseWhereClause($tokens, $pos);
            $where = $result['condition'];
            $pos = $result['pos'];
        }

        $data = ['table' => $table, 'where' => $where];
        // RETURNING
        if (kw($tokens, $pos, 'RETURNING')) {
            $pos++;
            if (sym($tokens, $pos, '*')) {
                $data['returning'] = ['*'];
            } else {
                $data['returning'] = [];
                while ($pos < count($tokens) && !sym($tokens, $pos, ';')) {
                    $data['returning'][] = $tokens[$pos]['value'];
                    $pos++;
                    if (sym($tokens, $pos, ',')) $pos++;
                }
            }
        }

        return new Event('delete_execute', $data);
    }
}
