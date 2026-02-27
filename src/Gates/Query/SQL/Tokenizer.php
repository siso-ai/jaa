<?php
/**
 * SQL Tokenizer — breaks SQL strings into typed tokens.
 *
 * Token types: KEYWORD, IDENTIFIER, NUMBER, STRING, OPERATOR, SYMBOL, BOOLEAN, NULL
 * Case-insensitive keyword matching. Identifiers preserve case.
 * Quoted identifiers: "col name" or `col name`.
 */
namespace Ice\Gates\Query\SQL;

const KEYWORDS = [
    'SELECT', 'FROM', 'WHERE', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'ILIKE', 'IS',
    'NULL', 'TRUE', 'FALSE', 'AS', 'ON', 'JOIN', 'INNER', 'LEFT', 'RIGHT',
    'FULL', 'OUTER', 'CROSS', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET',
    'DELETE', 'CREATE', 'DROP', 'TABLE', 'INDEX', 'VIEW', 'TRIGGER',
    'ALTER', 'ADD', 'CONSTRAINT', 'UNIQUE', 'CHECK', 'FOREIGN', 'KEY',
    'REFERENCES', 'PRIMARY', 'DEFAULT', 'IF', 'EXISTS', 'ORDER', 'BY',
    'ASC', 'DESC', 'LIMIT', 'OFFSET', 'GROUP', 'HAVING', 'DISTINCT',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'BETWEEN', 'CASE', 'WHEN',
    'THEN', 'ELSE', 'END', 'INTEGER', 'INT', 'TEXT', 'VARCHAR', 'REAL',
    'FLOAT', 'DOUBLE', 'BOOLEAN', 'BOOL', 'BLOB', 'DATE', 'TIMESTAMP',
    'AFTER', 'BEFORE', 'FOR', 'EACH', 'ROW', 'BEGIN',
    'COMMIT', 'ROLLBACK', 'RENAME', 'TO', 'COLUMN',
    'UNION', 'ALL', 'CAST', 'EXCEPT', 'INTERSECT',
    'EXPLAIN',
    'WITH', 'RECURSIVE', 'OVER', 'PARTITION', 'ROWS', 'RANGE', 'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT',
    'NULLS', 'FIRST', 'LAST',
    'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD',
    'GROUP_CONCAT', 'SEPARATOR',
    'CONFLICT', 'DO', 'NOTHING', 'RETURNING', 'TRUNCATE',
];

const OPERATORS = ['>=', '<=', '<>', '!=', '=', '<', '>', '||', '+', '/', '%'];

function tokenize(string $sql): array {
    static $keywordSet = null;
    if ($keywordSet === null) {
        $keywordSet = array_flip(KEYWORDS);
    }

    $tokens = [];
    $len = strlen($sql);
    $i = 0;

    while ($i < $len) {
        $ch = $sql[$i];

        // Skip whitespace
        if (ctype_space($ch)) { $i++; continue; }

        // Skip comments
        if ($ch === '-' && ($i + 1 < $len) && $sql[$i + 1] === '-') {
            while ($i < $len && $sql[$i] !== "\n") $i++;
            continue;
        }

        // String literal (single quotes)
        if ($ch === "'") {
            $val = '';
            $i++; // skip opening quote
            while ($i < $len) {
                if ($sql[$i] === "'" && ($i + 1 < $len) && $sql[$i + 1] === "'") {
                    $val .= "'"; $i += 2; // escaped quote
                } elseif ($sql[$i] === "'") {
                    break; // closing quote
                } else {
                    $val .= $sql[$i]; $i++;
                }
            }
            $i++; // skip closing quote
            $tokens[] = ['type' => 'STRING', 'value' => $val];
            continue;
        }

        // Quoted identifier (double quotes)
        if ($ch === '"') {
            $val = '';
            $i++;
            while ($i < $len && $sql[$i] !== '"') { $val .= $sql[$i]; $i++; }
            $i++;
            $tokens[] = ['type' => 'IDENTIFIER', 'value' => $val];
            continue;
        }

        // Backtick-quoted identifier
        if ($ch === '`') {
            $val = '';
            $i++;
            while ($i < $len && $sql[$i] !== '`') { $val .= $sql[$i]; $i++; }
            $i++;
            $tokens[] = ['type' => 'IDENTIFIER', 'value' => $val];
            continue;
        }

        // Operators (check multi-char first)
        $matchedOp = null;
        foreach (OPERATORS as $op) {
            $opLen = strlen($op);
            if (substr($sql, $i, $opLen) === $op) {
                $matchedOp = $op;
                break;
            }
        }
        if ($matchedOp !== null) {
            $tokens[] = ['type' => 'OPERATOR', 'value' => $matchedOp];
            $i += strlen($matchedOp);
            continue;
        }

        // Symbols
        if ($ch === '(' || $ch === ')' || $ch === ',' || $ch === '*' || $ch === '.' || $ch === ';') {
            $tokens[] = ['type' => 'SYMBOL', 'value' => $ch];
            $i++;
            continue;
        }

        // Numbers
        $isNeg = ($ch === '-' && ($i + 1 < $len) && ctype_digit($sql[$i + 1]) &&
                  (count($tokens) === 0 || in_array(end($tokens)['type'], ['OPERATOR', 'SYMBOL', 'KEYWORD'])));
        if (ctype_digit($ch) || $isNeg) {
            $num = '';
            if ($ch === '-') { $num .= '-'; $i++; }
            while ($i < $len && (ctype_digit($sql[$i]) || $sql[$i] === '.')) {
                $num .= $sql[$i]; $i++;
            }
            $tokens[] = [
                'type' => 'NUMBER',
                'value' => str_contains($num, '.') ? (float)$num : (int)$num,
            ];
            continue;
        }

        // Words (keywords or identifiers)
        if (ctype_alpha($ch) || $ch === '_') {
            $word = '';
            while ($i < $len && (ctype_alnum($sql[$i]) || $sql[$i] === '_')) {
                $word .= $sql[$i]; $i++;
            }
            $upper = strtoupper($word);

            if ($upper === 'TRUE' || $upper === 'FALSE') {
                $tokens[] = ['type' => 'BOOLEAN', 'value' => ($upper === 'TRUE')];
            } elseif ($upper === 'NULL') {
                $tokens[] = ['type' => 'NULL', 'value' => null];
            } elseif (isset($keywordSet[$upper])) {
                $tokens[] = ['type' => 'KEYWORD', 'value' => $upper];
            } else {
                $tokens[] = ['type' => 'IDENTIFIER', 'value' => $word];
            }
            continue;
        }

        // Minus sign as operator (when not comment or negative number)
        if ($ch === '-') {
            $tokens[] = ['type' => 'OPERATOR', 'value' => '-'];
            $i++;
            continue;
        }

        // Unknown character — skip
        $i++;
    }

    return $tokens;
}
