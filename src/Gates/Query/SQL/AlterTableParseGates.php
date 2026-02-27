<?php
/**
 * AlterTableParseGates — parses ALTER TABLE ADD COLUMN, DROP COLUMN, RENAME.
 */
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class AlterTableAddColumnParseGate extends PureGate {
    public function __construct() { parent::__construct('alter_table_add_column_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 2; // skip ALTER TABLE

        $table = $tokens[$pos]['value'];
        $pos++;

        $pos++; // skip ADD
        // Optional COLUMN keyword
        if (kw($tokens, $pos, 'COLUMN')) $pos++;

        $colName = $tokens[$pos]['value'];
        $pos++;

        $type = 'text';
        $typeKeywords = ['INTEGER','INT','TEXT','VARCHAR','REAL','FLOAT','DOUBLE','BOOLEAN','BOOL',
            'BLOB','DATE','TIMESTAMP','BIGINT','SMALLINT','NUMERIC','DECIMAL','CHAR','STRING'];
        if ($pos < count($tokens) && ($tokens[$pos]['type'] ?? '') === 'KEYWORD' &&
            in_array($tokens[$pos]['value'], $typeKeywords)) {
            $type = normalizeType($tokens[$pos]['value']);
            $pos++;
            if (sym($tokens, $pos, '(')) {
                while (!sym($tokens, $pos, ')') && $pos < count($tokens)) $pos++;
                $pos++;
            }
        }

        $nullable = true;
        $defaultVal = null;

        while ($pos < count($tokens) && !sym($tokens, $pos, ';')) {
            if (kw($tokens, $pos, 'NOT') && kw($tokens, $pos + 1, 'NULL')) {
                $nullable = false;
                $pos += 2;
            } elseif (kw($tokens, $pos, 'DEFAULT')) {
                $pos++;
                $lit = parseLiteralValue($tokens, $pos);
                $defaultVal = $lit['value'];
                $pos = $lit['pos'];
            } else {
                $pos++;
            }
        }

        return new Event('alter_table_add_column', [
            'table' => $table,
            'column' => ['name' => $colName, 'type' => $type, 'nullable' => $nullable, 'default' => $defaultVal],
        ]);
    }
}

class AlterTableDropColumnParseGate extends PureGate {
    public function __construct() { parent::__construct('alter_table_drop_column_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 2; // skip ALTER TABLE

        $table = $tokens[$pos]['value'];
        $pos++;

        $pos++; // skip DROP
        // Optional COLUMN keyword
        if (kw($tokens, $pos, 'COLUMN')) $pos++;

        $column = $tokens[$pos]['value'];

        return new Event('alter_table_drop_column', [
            'table' => $table, 'column' => $column,
        ]);
    }
}

class RenameTableParseGate extends PureGate {
    public function __construct() { parent::__construct('rename_table_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 2; // skip ALTER TABLE

        $table = $tokens[$pos]['value'];
        $pos++;

        $pos++; // skip RENAME
        // Optional TO keyword
        if (kw($tokens, $pos, 'TO')) $pos++;

        $newName = $tokens[$pos]['value'];

        return new Event('rename_table', [
            'table' => $table, 'newName' => $newName,
        ]);
    }
}
