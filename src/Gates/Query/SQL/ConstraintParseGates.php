<?php
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class ConstraintCreateParseGate extends PureGate {
    public function __construct() { parent::__construct('constraint_create_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 2; // skip ALTER TABLE

        $table = $tokens[$pos]['value'];
        $pos++;

        $pos++; // skip ADD
        $pos++; // skip CONSTRAINT

        $name = $tokens[$pos]['value'];
        $pos++;

        $type = 'check';
        $params = [];

        if (kw($tokens, $pos, 'UNIQUE')) {
            $type = 'unique';
            $pos++;
            if (sym($tokens, $pos, '(')) {
                $result = parseIdentList($tokens, $pos);
                $params = ['columns' => $result['idents']];
                $pos = $result['pos'];
            }
        } elseif (kw($tokens, $pos, 'CHECK')) {
            $type = 'check';
            $pos++;
            $parts = [];
            for ($j = $pos; $j < count($tokens); $j++) {
                $parts[] = is_bool($tokens[$j]['value']) ? ($tokens[$j]['value'] ? 'TRUE' : 'FALSE') : (string)($tokens[$j]['value'] ?? 'NULL');
            }
            $params = ['expression' => implode(' ', $parts)];
        } elseif (kw($tokens, $pos, 'FOREIGN')) {
            $type = 'foreign_key';
            $pos += 2; // FOREIGN KEY
            if (sym($tokens, $pos, '(')) {
                $cols = parseIdentList($tokens, $pos);
                $params['columns'] = $cols['idents'];
                $pos = $cols['pos'];
            }
            if (kw($tokens, $pos, 'REFERENCES')) {
                $pos++;
                $params['refTable'] = $tokens[$pos]['value'];
                $pos++;
                if (sym($tokens, $pos, '(')) {
                    $refCols = parseIdentList($tokens, $pos);
                    $params['refColumns'] = $refCols['idents'];
                }
            }
        }

        return new Event('constraint_create_execute', [
            'table' => $table, 'name' => $name, 'type' => $type, 'params' => $params,
        ]);
    }
}

class ConstraintDropParseGate extends PureGate {
    public function __construct() { parent::__construct('constraint_drop_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 2; // skip ALTER TABLE
        $table = $tokens[$pos]['value'];
        $pos++;
        $pos++; // DROP
        $pos++; // CONSTRAINT
        $name = $tokens[$pos]['value'];
        return new Event('constraint_drop_execute', ['table' => $table, 'name' => $name]);
    }
}
