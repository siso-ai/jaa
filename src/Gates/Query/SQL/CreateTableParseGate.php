<?php
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class CreateTableParseGate extends PureGate {
    public function __construct() { parent::__construct('create_table_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 0;

        $pos++; // CREATE
        $pos++; // TABLE
        $ifNotExists = false;
        if (kw($tokens, $pos, 'IF')) { $pos += 3; $ifNotExists = true; } // IF NOT EXISTS

        $table = $tokens[$pos]['value'];
        $pos++;

        // CREATE TABLE ... AS SELECT ...
        if (kw($tokens, $pos, 'AS')) {
            $pos++; // skip AS
            $subTokens = array_slice($tokens, $pos);
            return new Event('create_table_as_select', [
                'table' => $table,
                'selectTokens' => $subTokens,
                'ifNotExists' => $ifNotExists,
            ]);
        }

        $pos++; // (
        $columns = [];
        $count = count($tokens);

        while (!sym($tokens, $pos, ')') && $pos < $count) {
            // Skip table-level PRIMARY KEY(...)
            if (kw($tokens, $pos, 'PRIMARY')) {
                while ($pos < $count && !sym($tokens, $pos, ')')) $pos++;
                if (sym($tokens, $pos, ')') && isset($tokens[$pos + 1]) && $tokens[$pos + 1]['type'] !== 'SYMBOL') break;
                $pos++;
                if (sym($tokens, $pos, ',')) $pos++;
                continue;
            }

            $colName = $tokens[$pos]['value'];
            $pos++;

            $type = 'text';
            $typeKeywords = ['INTEGER','INT','TEXT','VARCHAR','REAL','FLOAT','DOUBLE','BOOLEAN','BOOL',
                'BLOB','DATE','TIMESTAMP','BIGINT','SMALLINT','NUMERIC','DECIMAL','CHAR','STRING'];
            if ($pos < $count && ($tokens[$pos]['type'] ?? '') === 'KEYWORD' && in_array($tokens[$pos]['value'], $typeKeywords)) {
                $type = normalizeType($tokens[$pos]['value']);
                $pos++;
                // Skip length specifiers: VARCHAR(255)
                if (sym($tokens, $pos, '(')) {
                    while (!sym($tokens, $pos, ')') && $pos < $count) $pos++;
                    $pos++; // skip )
                }
            }

            $nullable = true;
            $defaultVal = null;

            // Parse column constraints
            while ($pos < $count && !sym($tokens, $pos, ',') && !sym($tokens, $pos, ')')) {
                if (kw($tokens, $pos, 'NOT') && ($pos + 1 < $count) &&
                    (kw($tokens, $pos + 1, 'NULL') || match_token($tokens, $pos + 1, 'NULL'))) {
                    $nullable = false;
                    $pos += 2;
                } elseif (kw($tokens, $pos, 'PRIMARY') && kw($tokens, $pos + 1, 'KEY')) {
                    $nullable = false;
                    $pos += 2;
                } elseif (kw($tokens, $pos, 'DEFAULT')) {
                    $pos++;
                    $lit = parseLiteralValue($tokens, $pos);
                    $defaultVal = $lit['value'];
                    $pos = $lit['pos'];
                } elseif (kw($tokens, $pos, 'UNIQUE') || kw($tokens, $pos, 'CHECK') ||
                          kw($tokens, $pos, 'REFERENCES') || match_token($tokens, $pos, 'NULL') ||
                          match_token($tokens, $pos, 'KEYWORD', 'NULL')) {
                    $pos++;
                    if (sym($tokens, $pos, '(')) {
                        while (!sym($tokens, $pos, ')') && $pos < $count) $pos++;
                        $pos++;
                    }
                } else {
                    $pos++;
                }
            }

            $columns[] = ['name' => $colName, 'type' => $type, 'nullable' => $nullable, 'default' => $defaultVal];
            if (sym($tokens, $pos, ',')) $pos++;
        }

        return new Event('create_table_execute', ['table' => $table, 'columns' => $columns, 'ifNotExists' => $ifNotExists]);
    }
}
