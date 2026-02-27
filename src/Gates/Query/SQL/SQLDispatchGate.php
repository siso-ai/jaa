<?php
/**
 * SQLDispatchGate — routes SQL to the appropriate parse gate.
 * Tokenizes, examines first tokens, dispatches.
 */
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class SQLDispatchGate extends PureGate {
    public function __construct() { parent::__construct('sql'); }

    public function transform(Event $event): Event|array|null {
        $sql = trim($event->data['sql']);
        $tokens = tokenize($sql);
        if (count($tokens) === 0) {
            return new Event('error', ['message' => 'Empty SQL statement', 'source' => 'sql']);
        }

        $first = $tokens[0]['value'];
        $second = $tokens[1]['value'] ?? null;

        if ($first === 'CREATE' && $second === 'TABLE') return new Event('create_table_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'DROP' && $second === 'TABLE') return new Event('drop_table_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'INSERT') return new Event('insert_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'SELECT') return new Event('select_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'UPDATE') return new Event('update_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'DELETE') return new Event('delete_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'CREATE' && $second === 'UNIQUE') return new Event('index_create_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'CREATE' && $second === 'INDEX') return new Event('index_create_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'DROP' && $second === 'INDEX') return new Event('index_drop_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'CREATE' && $second === 'VIEW') return new Event('view_create_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'DROP' && $second === 'VIEW') return new Event('view_drop_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'CREATE' && $second === 'TRIGGER') return new Event('trigger_create_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'DROP' && $second === 'TRIGGER') return new Event('trigger_drop_parse', ['sql' => $sql, 'tokens' => $tokens]);
        if ($first === 'ALTER' && $second === 'TABLE') {
            $hasAdd = false; $hasDrop = false; $hasRename = false;
            $addIsConstraint = false; $dropIsConstraint = false;
            $addIsColumn = false; $dropIsColumn = false;
            for ($i = 0; $i < count($tokens) - 1; $i++) {
                if ($tokens[$i]['value'] === 'ADD' && ($tokens[$i + 1]['value'] ?? '') === 'CONSTRAINT') { $hasAdd = true; $addIsConstraint = true; }
                if ($tokens[$i]['value'] === 'DROP' && ($tokens[$i + 1]['value'] ?? '') === 'CONSTRAINT') { $hasDrop = true; $dropIsConstraint = true; }
                if ($tokens[$i]['value'] === 'ADD' && !$addIsConstraint) { $hasAdd = true; $addIsColumn = true; }
                if ($tokens[$i]['value'] === 'DROP' && !$dropIsConstraint) { $hasDrop = true; $dropIsColumn = true; }
                if ($tokens[$i]['value'] === 'RENAME') $hasRename = true;
            }
            if ($addIsConstraint) return new Event('constraint_create_parse', ['sql' => $sql, 'tokens' => $tokens]);
            if ($dropIsConstraint) return new Event('constraint_drop_parse', ['sql' => $sql, 'tokens' => $tokens]);
            if ($hasRename) return new Event('rename_table_parse', ['sql' => $sql, 'tokens' => $tokens]);
            if ($addIsColumn) return new Event('alter_table_add_column_parse', ['sql' => $sql, 'tokens' => $tokens]);
            if ($dropIsColumn) return new Event('alter_table_drop_column_parse', ['sql' => $sql, 'tokens' => $tokens]);
        }

        // Transaction support
        if ($first === 'BEGIN') return new Event('transaction_begin', []);
        if ($first === 'COMMIT') return new Event('transaction_commit', []);
        if ($first === 'ROLLBACK') return new Event('transaction_rollback', []);

        // TRUNCATE TABLE
        if ($first === 'TRUNCATE') {
            $pos = 1;
            if (kw($tokens, $pos, 'TABLE')) $pos++;
            $table = $tokens[$pos]['value'] ?? '';
            return new Event('delete_execute', ['table' => $table, 'where' => null]);
        }

        // WITH ... AS (Common Table Expressions)
        if ($first === 'WITH') {
            $ctes = [];
            $pos = 1;
            $recursive = false;
            if (kw($tokens, $pos, 'RECURSIVE')) { $recursive = true; $pos++; }
            $cteColumns = [];
            while ($pos < count($tokens)) {
                $cteName = $tokens[$pos]['value'];
                $pos++;
                // Optional column list: name(col1, col2, ...)
                $cols = null;
                if (sym($tokens, $pos, '(')) {
                    $identResult = parseIdentList($tokens, $pos);
                    $cols = $identResult['idents'];
                    $pos = $identResult['pos'];
                }
                if ($cols !== null) $cteColumns[$cteName] = $cols;
                if (kw($tokens, $pos, 'AS')) $pos++; // skip AS
                if (sym($tokens, $pos, '(')) {
                    $sub = parseSubquery($tokens, $pos);
                    $ctes[$cteName] = $sub['tokens'];
                    $pos = $sub['pos'];
                }
                if (sym($tokens, $pos, ',')) { $pos++; continue; }
                break; // No more CTEs
            }
            // Remaining tokens are the main query
            $mainTokens = array_values(array_slice($tokens, $pos));
            if (count($mainTokens) > 0 && $mainTokens[0]['value'] === 'SELECT') {
                $data = ['sql' => $sql, 'tokens' => $mainTokens, 'ctes' => $ctes];
                if ($recursive) $data['recursive'] = true;
                if (count($cteColumns) > 0) $data['cteColumns'] = $cteColumns;
                return new Event('select_parse', $data);
            }
            return new Event('error', ['message' => 'WITH must be followed by SELECT', 'source' => 'sql']);
        }

        // EXPLAIN: strip EXPLAIN keyword and re-dispatch, marking for introspection
        if ($first === 'EXPLAIN') {
            $innerTokens = array_slice($tokens, 1);
            $innerSql = implode(' ', array_map(fn($t) => $t['value'], $innerTokens));
            return new Event('explain', ['sql' => $innerSql, 'tokens' => $innerTokens]);
        }

        return new Event('error', ['message' => "Unrecognized SQL: {$sql}", 'source' => 'sql']);
    }
}
