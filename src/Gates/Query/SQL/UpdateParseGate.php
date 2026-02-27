<?php
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class UpdateParseGate extends PureGate {
    public function __construct() { parent::__construct('update_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 1; // skip UPDATE

        $table = $tokens[$pos]['value'];
        $pos++;

        $pos++; // skip SET

        $changes = [];
        $changesExprs = [];
        while ($pos < count($tokens) && !kw($tokens, $pos, 'WHERE') && !kw($tokens, $pos, 'RETURNING') && !kw($tokens, $pos, 'FROM') && !sym($tokens, $pos, ';')) {
            $column = $tokens[$pos]['value'];
            $pos++;
            $pos++; // skip =
            $exprResult = parseExpression($tokens, $pos);
            $expr = $exprResult['expr'];
            $pos = $exprResult['pos'];

            // If it's a simple literal, use the old changes format for backward compat
            if (is_array($expr) && isset($expr['literal'])) {
                $changes[$column] = $expr['literal'];
            } else {
                $changesExprs[$column] = $expr;
            }
            if (sym($tokens, $pos, ',')) $pos++;
        }

        // FROM clause (PostgreSQL-style join update)
        $fromTable = null;
        $fromAlias = null;
        if (kw($tokens, $pos, 'FROM')) {
            $pos++;
            $fromTable = $tokens[$pos]['value'];
            $pos++;
            $fromAlias = $fromTable;
            // Optional alias
            if (isset($tokens[$pos]) && $tokens[$pos]['type'] === 'IDENTIFIER' &&
                !kw($tokens, $pos, 'WHERE') && !kw($tokens, $pos, 'RETURNING') && !kw($tokens, $pos, 'SET')) {
                if (kw($tokens, $pos, 'AS')) $pos++;
                $fromAlias = $tokens[$pos]['value'];
                $pos++;
            }
        }

        $where = null;
        if (kw($tokens, $pos, 'WHERE')) {
            $pos++;
            $result = parseWhereClause($tokens, $pos);
            $where = $result['condition'];
            $pos = $result['pos'];
        }

        $data = ['table' => $table, 'changes' => $changes, 'where' => $where];
        if (count($changesExprs) > 0) {
            $data['changesExprs'] = $changesExprs;
        }
        if ($fromTable !== null) {
            $data['fromTable'] = $fromTable;
            $data['fromAlias'] = $fromAlias;
        }

        // RETURNING
        if (kw($tokens, $pos, 'RETURNING')) {
            $pos++;
            $data['returning'] = self::parseReturningCols($tokens, $pos);
        }

        return new Event('update_execute', $data);
    }

    private static function parseReturningCols(array $tokens, int $pos): array {
        if (sym($tokens, $pos, '*')) return ['*'];
        $cols = [];
        while ($pos < count($tokens) && !sym($tokens, $pos, ';')) {
            $cols[] = $tokens[$pos]['value'];
            $pos++;
            if (sym($tokens, $pos, ',')) $pos++;
        }
        return $cols;
    }
}
