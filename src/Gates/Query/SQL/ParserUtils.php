<?php
/**
 * SQL Parser Utilities — shared parsing functions.
 * Every function takes (tokens, pos) and returns associative array with result + pos.
 * Pure. Stateless. Used by all parse gates.
 */
namespace Ice\Gates\Query\SQL;

/** Check if token at pos matches type and optionally value */
function match_token(array $tokens, int $pos, string $type, mixed $value = null): bool {
    if ($pos >= count($tokens)) return false;
    $t = $tokens[$pos];
    if ($t['type'] !== $type) return false;
    if ($value !== null && $t['value'] !== $value) return false;
    return true;
}

/** Check if token at pos is a keyword with given value */
function kw(array $tokens, int $pos, string $value): bool {
    return match_token($tokens, $pos, 'KEYWORD', $value);
}

/** Check if token at pos is a symbol with given value */
function sym(array $tokens, int $pos, string $value): bool {
    return match_token($tokens, $pos, 'SYMBOL', $value);
}

/** Expect a specific token, throw if not found */
function expect(array $tokens, int $pos, string $type, mixed $value = null): int {
    if (!match_token($tokens, $pos, $type, $value)) {
        $actual = $pos < count($tokens)
            ? "{$tokens[$pos]['type']}:{$tokens[$pos]['value']}"
            : 'EOF';
        throw new \RuntimeException("Expected {$type}:{$value}, got {$actual} at position {$pos}");
    }
    return $pos + 1;
}

/** Get value at pos */
function val(array $tokens, int $pos): mixed {
    return $tokens[$pos]['value'] ?? null;
}

/** Parse a column list: col1, col2, col3 or * */
function parseColumnList(array $tokens, int $pos): array {
    $columns = [];

    if (sym($tokens, $pos, '*')) {
        return ['columns' => ['*'], 'pos' => $pos + 1];
    }

    while ($pos < count($tokens)) {
        $col = parseSelectColumn($tokens, $pos);
        $columns[] = $col['column'];
        $pos = $col['pos'];

        if (sym($tokens, $pos, ',')) {
            $pos++; // skip comma
        } else {
            break;
        }
    }

    return ['columns' => $columns, 'pos' => $pos];
}

/** Parse a single column expression in SELECT */
function parseSelectColumn(array $tokens, int $pos): array {
    // Star
    if (sym($tokens, $pos, '*')) {
        return ['column' => '*', 'pos' => $pos + 1];
    }

    // Scalar subquery: (SELECT ...)
    if (isSubquery($tokens, $pos)) {
        $sub = parseSubquery($tokens, $pos);
        $alias = 'subquery';
        if (kw($tokens, $sub['pos'], 'AS')) {
            $sub['pos']++;
            $alias = $tokens[$sub['pos']]['value'];
            $sub['pos']++;
        }
        return ['column' => ['expr' => ['subquery' => $sub['tokens']], 'alias' => $alias], 'pos' => $sub['pos']];
    }

    // Aggregate/Window functions: COUNT, SUM, AVG, MIN, MAX, ROW_NUMBER, RANK, DENSE_RANK, GROUP_CONCAT
    $aggFns = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'];
    $winFns = ['ROW_NUMBER', 'RANK', 'DENSE_RANK', 'NTILE', 'LAG', 'LEAD'];
    $allSpecialFns = array_merge($aggFns, $winFns, ['GROUP_CONCAT']);
    $tokVal = $tokens[$pos]['value'] ?? '';

    if (in_array($tokVal, $allSpecialFns) && sym($tokens, $pos + 1, '(')) {
        $fn = $tokVal;
        $pos++; // skip fn name
        $pos = expect($tokens, $pos, 'SYMBOL', '(');

        // Parse function arguments
        $column = null;
        $distinct = false;
        $separator = ',';

        if (sym($tokens, $pos, '*')) {
            $column = '*';
            $pos++;
        } elseif (sym($tokens, $pos, ')')) {
            // No args: ROW_NUMBER(), RANK(), etc.
            $column = '*';
        } else {
            // Check for DISTINCT
            if (kw($tokens, $pos, 'DISTINCT')) {
                $distinct = true;
                $pos++;
            }
            $column = $tokens[$pos]['value'];
            $pos++;
            // Handle qualified column: table.column
            if (sym($tokens, $pos, '.')) {
                $pos++;
                $column = $column . '.' . $tokens[$pos]['value'];
                $pos++;
            }
            // GROUP_CONCAT SEPARATOR
            if ($fn === 'GROUP_CONCAT' && kw($tokens, $pos, 'SEPARATOR')) {
                $pos++;
                $separator = $tokens[$pos]['value'];
                $pos++;
            }
        }
        $pos = expect($tokens, $pos, 'SYMBOL', ')');

        // Check for OVER → window function
        if (kw($tokens, $pos, 'OVER')) {
            $pos++;
            $over = parseWindowSpec($tokens, $pos);
            $pos = $over['pos'];
            $alias = strtolower($fn);
            if (kw($tokens, $pos, 'AS')) {
                $pos++;
                $alias = $tokens[$pos]['value'];
                $pos++;
            }
            return [
                'column' => ['window' => ['fn' => $fn, 'column' => $column, 'distinct' => $distinct, 'over' => $over['spec']], 'alias' => $alias],
                'pos' => $pos,
            ];
        }

        // Regular aggregate
        $alias = strtolower($fn) . '_' . $column;
        if (kw($tokens, $pos, 'AS')) {
            $pos++;
            $alias = $tokens[$pos]['value'];
            $pos++;
        }

        if ($fn === 'GROUP_CONCAT') {
            return [
                'column' => ['aggregate' => ['fn' => 'GROUP_CONCAT', 'column' => $column, 'distinct' => $distinct, 'separator' => $separator], 'alias' => $alias],
                'pos' => $pos,
            ];
        }

        return [
            'column' => ['aggregate' => ['fn' => $fn, 'column' => $column, 'distinct' => $distinct], 'alias' => $alias],
            'pos' => $pos,
        ];
    }

    // General expression (may be column name, arithmetic, function call, etc.)
    $exprResult = parseExpression($tokens, $pos);
    $expr = $exprResult['expr'];
    $pos = $exprResult['pos'];

    // Check for alias
    $alias = null;
    if (kw($tokens, $pos, 'AS')) {
        $pos++;
        $alias = $tokens[$pos]['value'];
        $pos++;
    }

    // Simple column reference — no alias needed
    if (is_string($expr) && $alias === null) {
        return ['column' => $expr, 'pos' => $pos];
    }

    // Expression or aliased column
    if ($alias === null) {
        // Generate alias from expression
        if (is_string($expr)) {
            $alias = $expr;
        } else {
            $alias = 'expr_' . $pos;
        }
    }

    // If expression is a simple column name string with alias, wrap as expr
    if (is_string($expr)) {
        return ['column' => ['expr' => $expr, 'alias' => $alias], 'pos' => $pos];
    }

    return ['column' => ['expr' => $expr, 'alias' => $alias], 'pos' => $pos];
}

/** Parse a WHERE clause into a condition tree */
function parseWhereClause(array $tokens, int $pos): array {
    return parseOrExpr($tokens, $pos);
}

function parseOrExpr(array $tokens, int $pos): array {
    $result = parseAndExpr($tokens, $pos);
    $left = $result['condition'];
    $pos = $result['pos'];

    while (kw($tokens, $pos, 'OR')) {
        $pos++;
        $right = parseAndExpr($tokens, $pos);
        $left = ['or' => [$left, $right['condition']]];
        $pos = $right['pos'];
    }

    return ['condition' => $left, 'pos' => $pos];
}

function parseAndExpr(array $tokens, int $pos): array {
    $result = parseNotExpr($tokens, $pos);
    $left = $result['condition'];
    $pos = $result['pos'];

    while (kw($tokens, $pos, 'AND')) {
        $pos++;
        $right = parseNotExpr($tokens, $pos);
        $left = ['and' => [$left, $right['condition']]];
        $pos = $right['pos'];
    }

    return ['condition' => $left, 'pos' => $pos];
}

function parseNotExpr(array $tokens, int $pos): array {
    if (kw($tokens, $pos, 'NOT')) {
        $pos++;
        // NOT EXISTS
        if (kw($tokens, $pos, 'EXISTS') && isSubquery($tokens, $pos + 1)) {
            $pos++; // skip EXISTS
            $sub = parseSubquery($tokens, $pos);
            return ['condition' => ['not' => ['exists' => true, 'subquery' => $sub['tokens']]], 'pos' => $sub['pos']];
        }
        $result = parseComparison($tokens, $pos);
        return ['condition' => ['not' => $result['condition']], 'pos' => $result['pos']];
    }
    // EXISTS (SELECT ...)
    if (kw($tokens, $pos, 'EXISTS') && isSubquery($tokens, $pos + 1)) {
        $pos++; // skip EXISTS
        $sub = parseSubquery($tokens, $pos);
        return ['condition' => ['exists' => true, 'subquery' => $sub['tokens']], 'pos' => $sub['pos']];
    }
    return parseComparison($tokens, $pos);
}

function parseComparison(array $tokens, int $pos): array {
    // Parenthesized condition or subquery
    if (sym($tokens, $pos, '(')) {
        if (kw($tokens, $pos + 1, 'SELECT')) {
            // Scalar subquery comparison: (SELECT ...) = value
            $sub = parseSubquery($tokens, $pos);
            // If followed by operator, treat as scalar subquery comparison
            if (isset($tokens[$sub['pos']]) && $tokens[$sub['pos']]['type'] === 'OPERATOR') {
                $op = $tokens[$sub['pos']]['value'];
                $pos = $sub['pos'] + 1;
                $rightResult = parseExpression($tokens, $pos);
                return [
                    'condition' => [
                        'leftExpr' => ['subquery' => $sub['tokens']],
                        'op' => $op,
                        'rightExpr' => $rightResult['expr'],
                    ],
                    'pos' => $rightResult['pos'],
                ];
            }
            // Standalone scalar subquery in boolean context — treat as EXISTS
            return ['condition' => ['exists' => true, 'subquery' => $sub['tokens']], 'pos' => $sub['pos']];
        }
        $pos++;
        $result = parseOrExpr($tokens, $pos);
        if (sym($tokens, $result['pos'], ')')) {
            return ['condition' => $result['condition'], 'pos' => $result['pos'] + 1];
        }
        return $result;
    }

    // Parse left side as expression
    $leftResult = parseExpression($tokens, $pos);
    $leftExpr = $leftResult['expr'];
    $pos = $leftResult['pos'];

    // Determine if left side is a simple column name
    $isSimpleColumn = is_string($leftExpr);
    $column = $isSimpleColumn ? $leftExpr : null;

    // IS NULL / IS NOT NULL
    if (kw($tokens, $pos, 'IS')) {
        $pos++;
        if (kw($tokens, $pos, 'NOT')) {
            $pos++;
            $pos++; // skip NULL
            if ($isSimpleColumn) {
                return ['condition' => ['column' => $column, 'op' => 'is_not_null'], 'pos' => $pos];
            }
            return ['condition' => ['leftExpr' => $leftExpr, 'op' => 'is_not_null'], 'pos' => $pos];
        }
        $pos++; // skip NULL
        if ($isSimpleColumn) {
            return ['condition' => ['column' => $column, 'op' => 'is_null'], 'pos' => $pos];
        }
        return ['condition' => ['leftExpr' => $leftExpr, 'op' => 'is_null'], 'pos' => $pos];
    }

    // NOT IN / NOT LIKE / NOT ILIKE / NOT BETWEEN
    if (kw($tokens, $pos, 'NOT')) {
        $pos++;
        if (kw($tokens, $pos, 'IN')) {
            $pos++;
            if (isSubquery($tokens, $pos)) {
                $sub = parseSubquery($tokens, $pos);
                $cond = $isSimpleColumn
                    ? ['column' => $column, 'op' => 'in', 'subquery' => $sub['tokens']]
                    : ['leftExpr' => $leftExpr, 'op' => 'in', 'subquery' => $sub['tokens']];
                return ['condition' => ['not' => $cond], 'pos' => $sub['pos']];
            }
            $list = parseInList($tokens, $pos);
            $cond = $isSimpleColumn
                ? ['column' => $column, 'op' => 'in', 'value' => $list['values']]
                : ['leftExpr' => $leftExpr, 'op' => 'in', 'value' => $list['values']];
            return ['condition' => ['not' => $cond], 'pos' => $list['pos']];
        }
        if (kw($tokens, $pos, 'LIKE') || kw($tokens, $pos, 'ILIKE')) {
            $opType = strtolower($tokens[$pos]['value']);
            $pos++;
            $pattern = $tokens[$pos]['value'];
            $pos++;
            $cond = $isSimpleColumn
                ? ['column' => $column, 'op' => $opType, 'value' => $pattern]
                : ['leftExpr' => $leftExpr, 'op' => $opType, 'value' => $pattern];
            return ['condition' => ['not' => $cond], 'pos' => $pos];
        }
        if (kw($tokens, $pos, 'BETWEEN')) {
            $pos++;
            $low = parseLiteralValue($tokens, $pos);
            $pos = $low['pos'];
            $pos++; // skip AND
            $high = parseLiteralValue($tokens, $pos);
            $pos = $high['pos'];
            if ($isSimpleColumn) {
                return [
                    'condition' => ['not' => ['and' => [
                        ['column' => $column, 'op' => '>=', 'value' => $low['value']],
                        ['column' => $column, 'op' => '<=', 'value' => $high['value']],
                    ]]],
                    'pos' => $pos,
                ];
            }
            return [
                'condition' => ['not' => ['and' => [
                    ['leftExpr' => $leftExpr, 'op' => '>=', 'rightExpr' => ['literal' => $low['value']]],
                    ['leftExpr' => $leftExpr, 'op' => '<=', 'rightExpr' => ['literal' => $high['value']]],
                ]]],
                'pos' => $pos,
            ];
        }
    }

    // IN (with subquery support)
    if (kw($tokens, $pos, 'IN')) {
        $pos++;
        if (isSubquery($tokens, $pos)) {
            $sub = parseSubquery($tokens, $pos);
            $cond = $isSimpleColumn
                ? ['column' => $column, 'op' => 'in', 'subquery' => $sub['tokens']]
                : ['leftExpr' => $leftExpr, 'op' => 'in', 'subquery' => $sub['tokens']];
            return ['condition' => $cond, 'pos' => $sub['pos']];
        }
        $list = parseInList($tokens, $pos);
        $cond = $isSimpleColumn
            ? ['column' => $column, 'op' => 'in', 'value' => $list['values']]
            : ['leftExpr' => $leftExpr, 'op' => 'in', 'value' => $list['values']];
        return ['condition' => $cond, 'pos' => $list['pos']];
    }

    // LIKE / ILIKE
    if (kw($tokens, $pos, 'LIKE') || kw($tokens, $pos, 'ILIKE')) {
        $opType = strtolower($tokens[$pos]['value']);
        $pos++;
        $pattern = $tokens[$pos]['value'];
        $pos++;
        $cond = $isSimpleColumn
            ? ['column' => $column, 'op' => $opType, 'value' => $pattern]
            : ['leftExpr' => $leftExpr, 'op' => $opType, 'value' => $pattern];
        return ['condition' => $cond, 'pos' => $pos];
    }

    // BETWEEN
    if (kw($tokens, $pos, 'BETWEEN')) {
        $pos++;
        $low = parseLiteralValue($tokens, $pos);
        $pos = $low['pos'];
        $pos++; // skip AND
        $high = parseLiteralValue($tokens, $pos);
        $pos = $high['pos'];
        if ($isSimpleColumn) {
            return [
                'condition' => ['and' => [
                    ['column' => $column, 'op' => '>=', 'value' => $low['value']],
                    ['column' => $column, 'op' => '<=', 'value' => $high['value']],
                ]],
                'pos' => $pos,
            ];
        }
        return [
            'condition' => ['and' => [
                ['leftExpr' => $leftExpr, 'op' => '>=', 'rightExpr' => ['literal' => $low['value']]],
                ['leftExpr' => $leftExpr, 'op' => '<=', 'rightExpr' => ['literal' => $high['value']]],
            ]],
            'pos' => $pos,
        ];
    }

    // Standard comparison operators: =, !=, <, >, <=, >=, <>
    $op = $tokens[$pos]['value'] ?? '';
    $pos++;

    // Parse right side as expression
    $rightResult = parseExpression($tokens, $pos);
    $rightExpr = $rightResult['expr'];
    $pos = $rightResult['pos'];

    // If both sides are simple (column = literal), use old format for compatibility
    $isSimpleRight = is_array($rightExpr) && isset($rightExpr['literal']);
    if ($isSimpleColumn && $isSimpleRight) {
        return ['condition' => ['column' => $column, 'op' => $op, 'value' => $rightExpr['literal']], 'pos' => $pos];
    }

    // Use expression format
    if ($isSimpleColumn) {
        $leftExpr = $column; // column ref as string
    }
    return ['condition' => ['leftExpr' => $leftExpr, 'op' => $op, 'rightExpr' => $rightExpr], 'pos' => $pos];
}

function parseInList(array $tokens, int $pos): array {
    $pos = expect($tokens, $pos, 'SYMBOL', '(');
    $values = [];
    while (!sym($tokens, $pos, ')')) {
        $v = parseLiteralValue($tokens, $pos);
        $values[] = $v['value'];
        $pos = $v['pos'];
        if (sym($tokens, $pos, ',')) $pos++;
    }
    $pos++; // skip )
    return ['values' => $values, 'pos' => $pos];
}

/** Parse a literal value: number, string, boolean, null */
function parseLiteralValue(array $tokens, int $pos): array {
    $t = $tokens[$pos];
    if ($t['type'] === 'NUMBER') return ['value' => $t['value'], 'pos' => $pos + 1];
    if ($t['type'] === 'STRING') return ['value' => $t['value'], 'pos' => $pos + 1];
    if ($t['type'] === 'BOOLEAN') return ['value' => $t['value'], 'pos' => $pos + 1];
    if ($t['type'] === 'NULL') return ['value' => null, 'pos' => $pos + 1];
    if ($t['type'] === 'KEYWORD' && $t['value'] === 'DEFAULT') return ['value' => null, 'pos' => $pos + 1];
    // Negative number
    if ($t['type'] === 'OPERATOR' && $t['value'] === '-' && isset($tokens[$pos + 1]) && $tokens[$pos + 1]['type'] === 'NUMBER') {
        return ['value' => -$tokens[$pos + 1]['value'], 'pos' => $pos + 2];
    }
    throw new \RuntimeException("Expected literal value, got {$t['type']}:{$t['value']} at position {$pos}");
}

/** Parse ORDER BY clause */
function parseOrderBy(array $tokens, int $pos): array {
    $order = [];
    while ($pos < count($tokens)) {
        $column = $tokens[$pos]['value'];
        $pos++;
        // Handle qualified: table.column
        if (sym($tokens, $pos, '.')) {
            $pos++;
            $column = $column . '.' . $tokens[$pos]['value'];
            $pos++;
        }
        $direction = 'asc';
        if (kw($tokens, $pos, 'ASC')) { $direction = 'asc'; $pos++; }
        elseif (kw($tokens, $pos, 'DESC')) { $direction = 'desc'; $pos++; }
        $nulls = null;
        if (kw($tokens, $pos, 'NULLS')) {
            $pos++;
            if (kw($tokens, $pos, 'FIRST')) { $nulls = 'first'; $pos++; }
            elseif (kw($tokens, $pos, 'LAST')) { $nulls = 'last'; $pos++; }
        }
        $entry = ['column' => $column, 'direction' => $direction];
        if ($nulls !== null) $entry['nulls'] = $nulls;
        $order[] = $entry;
        if (sym($tokens, $pos, ',')) { $pos++; } else { break; }
    }
    return ['order' => $order, 'pos' => $pos];
}

/** Parse a table reference with optional alias */
function parseTableRef(array $tokens, int $pos): array {
    $table = $tokens[$pos]['value'];
    $pos++;
    $alias = null;
    if (kw($tokens, $pos, 'AS')) {
        $pos++;
        $alias = $tokens[$pos]['value'];
        $pos++;
    } elseif (isset($tokens[$pos]) && $tokens[$pos]['type'] === 'IDENTIFIER' &&
              !kw($tokens, $pos, 'WHERE') && !kw($tokens, $pos, 'ON') &&
              !kw($tokens, $pos, 'SET') && !kw($tokens, $pos, 'ORDER') &&
              !kw($tokens, $pos, 'GROUP') && !kw($tokens, $pos, 'LIMIT') &&
              !kw($tokens, $pos, 'JOIN') && !kw($tokens, $pos, 'INNER') &&
              !kw($tokens, $pos, 'LEFT') && !kw($tokens, $pos, 'RIGHT') &&
              !kw($tokens, $pos, 'FULL') && !kw($tokens, $pos, 'CROSS') &&
              !kw($tokens, $pos, 'HAVING')) {
        $alias = $tokens[$pos]['value'];
        $pos++;
    }
    return ['table' => $table, 'alias' => $alias, 'pos' => $pos];
}

/** Parse a comma-separated value list: (val1, val2, ...) */
function parseValueList(array $tokens, int $pos): array {
    $pos = expect($tokens, $pos, 'SYMBOL', '(');
    $values = [];
    $exprs = [];
    $hasExprs = false;
    while (!sym($tokens, $pos, ')')) {
        // Try expression parsing
        $exprResult = parseExpression($tokens, $pos);
        $expr = $exprResult['expr'];
        $pos = $exprResult['pos'];
        // If it's a simple literal, extract value
        if (is_array($expr) && isset($expr['literal'])) {
            $values[] = $expr['literal'];
            $exprs[] = $expr;
        } else {
            $values[] = null; // placeholder
            $exprs[] = $expr;
            $hasExprs = true;
        }
        if (sym($tokens, $pos, ',')) $pos++;
    }
    $pos++; // skip )
    $result = ['values' => $values, 'pos' => $pos];
    if ($hasExprs) $result['exprs'] = $exprs;
    return $result;
}

/** Parse a parenthesized identifier list: (col1, col2) */
function parseIdentList(array $tokens, int $pos): array {
    $pos = expect($tokens, $pos, 'SYMBOL', '(');
    $idents = [];
    while (!sym($tokens, $pos, ')')) {
        $idents[] = $tokens[$pos]['value'];
        $pos++;
        if (sym($tokens, $pos, ',')) $pos++;
    }
    $pos++; // skip )
    return ['idents' => $idents, 'pos' => $pos];
}

/** Map SQL type names to normalized types */
function normalizeType(string $typeName): string {
    $map = [
        'INTEGER' => 'integer', 'INT' => 'integer', 'BIGINT' => 'integer', 'SMALLINT' => 'integer',
        'TEXT' => 'text', 'VARCHAR' => 'text', 'CHAR' => 'text', 'STRING' => 'text',
        'REAL' => 'real', 'FLOAT' => 'real', 'DOUBLE' => 'real', 'NUMERIC' => 'real', 'DECIMAL' => 'real',
        'BOOLEAN' => 'boolean', 'BOOL' => 'boolean',
        'BLOB' => 'blob',
        'DATE' => 'date', 'TIMESTAMP' => 'timestamp',
    ];
    return $map[strtoupper($typeName)] ?? 'text';
}

// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
// SQL Expression Parser — recursive descent
// Precedence (low→high): || (concat), +/-, */%, unary -, atom
// Returns: ['expr' => exprAST, 'pos' => int]
// Expression AST matches the shapes evaluateExpression() handles:
//   string              → column reference
//   ['literal' => val]  → literal value
//   ['op','left','right'] → arithmetic
//   ['fn','args']       → function call
//   ['case','else']     → CASE WHEN
// ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

/** Known SQL functions (non-aggregate) */
function isSQLFunction(string $name): bool {
    static $fns = ['UPPER','LOWER','LENGTH','ABS','ROUND','CONCAT','SUBSTR','SUBSTRING',
        'IFNULL','NULLIF','REPLACE','TRIM','LTRIM','RTRIM','COALESCE','CAST',
        'LEFT','RIGHT','REVERSE','REPEAT','LPAD','RPAD','POSITION','INSTR',
        'CHAR_LENGTH','CHARACTER_LENGTH','STARTS_WITH','ENDS_WITH',
        'CEIL','CEILING','FLOOR','POWER','POW','SQRT','MOD','SIGN',
        'LOG','LN','EXP','PI','RANDOM','RAND',
        'TYPEOF','GREATEST','LEAST',
        'IIF','DATE','TIME','DATETIME','STRFTIME','NOW',
        'CURRENT_TIMESTAMP','CURRENT_DATE','CURRENT_TIME',
        'MAX','MIN','SUM','AVG','COUNT'];
    return in_array(strtoupper($name), $fns);
}

/** Top-level expression: handles || (string concatenation) */
function parseExpression(array $tokens, int $pos): array {
    $result = parseAdditiveExpr($tokens, $pos);
    $expr = $result['expr'];
    $pos = $result['pos'];

    // || string concatenation
    while ($pos < count($tokens) && isset($tokens[$pos]) &&
           ($tokens[$pos]['value'] ?? '') === '||') {
        $pos++;
        $right = parseAdditiveExpr($tokens, $pos);
        $expr = ['fn' => 'CONCAT', 'args' => [$expr, $right['expr']]];
        $pos = $right['pos'];
    }

    return ['expr' => $expr, 'pos' => $pos];
}

/** Additive: +, - */
function parseAdditiveExpr(array $tokens, int $pos): array {
    $result = parseMultiplicativeExpr($tokens, $pos);
    $expr = $result['expr'];
    $pos = $result['pos'];

    while ($pos < count($tokens) && isset($tokens[$pos]) &&
           in_array($tokens[$pos]['value'] ?? '', ['+', '-']) &&
           ($tokens[$pos]['type'] ?? '') === 'OPERATOR') {
        $op = $tokens[$pos]['value'];
        $pos++;
        $right = parseMultiplicativeExpr($tokens, $pos);
        $expr = ['op' => $op, 'left' => $expr, 'right' => $right['expr']];
        $pos = $right['pos'];
    }

    return ['expr' => $expr, 'pos' => $pos];
}

/** Multiplicative: *, /, % */
function parseMultiplicativeExpr(array $tokens, int $pos): array {
    $result = parseUnaryExpr($tokens, $pos);
    $expr = $result['expr'];
    $pos = $result['pos'];

    while ($pos < count($tokens) && isset($tokens[$pos]) &&
           in_array($tokens[$pos]['value'] ?? '', ['*', '/', '%'])) {
        // * as SYMBOL (in SELECT *) should not be captured here
        if (($tokens[$pos]['type'] ?? '') === 'SYMBOL' && $tokens[$pos]['value'] === '*') {
            // Only treat as multiply if previous token was a number, identifier, or close-paren
            if ($pos > 0) {
                $prevType = $tokens[$pos - 1]['type'] ?? '';
                if (!in_array($prevType, ['NUMBER', 'IDENTIFIER', 'SYMBOL'])) break;
                if ($prevType === 'SYMBOL' && $tokens[$pos - 1]['value'] !== ')') break;
            } else {
                break;
            }
        }
        $op = $tokens[$pos]['value'];
        $pos++;
        $right = parseUnaryExpr($tokens, $pos);
        $expr = ['op' => $op, 'left' => $expr, 'right' => $right['expr']];
        $pos = $right['pos'];
    }

    return ['expr' => $expr, 'pos' => $pos];
}

/** Unary: negation */
function parseUnaryExpr(array $tokens, int $pos): array {
    if ($pos < count($tokens) && ($tokens[$pos]['value'] ?? '') === '-' &&
        ($tokens[$pos]['type'] ?? '') === 'OPERATOR') {
        $pos++;
        $result = parseUnaryExpr($tokens, $pos);
        // Optimize: negative literal → just negate
        if (is_array($result['expr']) && isset($result['expr']['literal']) && is_numeric($result['expr']['literal'])) {
            return ['expr' => ['literal' => -$result['expr']['literal']], 'pos' => $result['pos']];
        }
        return ['expr' => ['op' => '-', 'left' => ['literal' => 0], 'right' => $result['expr']], 'pos' => $result['pos']];
    }
    return parseAtomExpr($tokens, $pos);
}

/** Atom: literal, column, function call, parens, CASE WHEN */
function parseAtomExpr(array $tokens, int $pos): array {
    if ($pos >= count($tokens)) {
        throw new \RuntimeException("Unexpected end of expression at position $pos");
    }

    $token = $tokens[$pos];

    // CASE WHEN expression
    if (kw($tokens, $pos, 'CASE')) {
        return parseCaseExpr($tokens, $pos);
    }

    // Parenthesized expression or subquery
    if (sym($tokens, $pos, '(')) {
        // Scalar subquery: (SELECT ...)
        if (isSubquery($tokens, $pos)) {
            $sub = parseSubquery($tokens, $pos);
            return ['expr' => ['subquery' => $sub['tokens']], 'pos' => $sub['pos']];
        }
        $pos++;
        $result = parseExpression($tokens, $pos);
        if (sym($tokens, $result['pos'], ')')) {
            return ['expr' => $result['expr'], 'pos' => $result['pos'] + 1];
        }
        return $result;
    }

    // Number literal
    if ($token['type'] === 'NUMBER') {
        $val = strpos($token['value'], '.') !== false ? (float)$token['value'] : (int)$token['value'];
        return ['expr' => ['literal' => $val], 'pos' => $pos + 1];
    }

    // String literal
    if ($token['type'] === 'STRING') {
        return ['expr' => ['literal' => $token['value']], 'pos' => $pos + 1];
    }

    // Boolean
    if ($token['type'] === 'BOOLEAN') {
        return ['expr' => ['literal' => ($token['value'] === true || $token['value'] === 'TRUE')], 'pos' => $pos + 1];
    }

    // NULL
    if ($token['type'] === 'NULL') {
        return ['expr' => ['literal' => null], 'pos' => $pos + 1];
    }

    // IIF(condition, then, else) → CASE WHEN condition THEN then ELSE else END
    if (strtoupper($token['value'] ?? '') === 'IIF' && sym($tokens, $pos + 1, '(')) {
        $pos += 2; // skip IIF(
        // Parse condition using WHERE clause parser
        $condResult = parseWhereClause($tokens, $pos);
        $cond = $condResult['condition'];
        $pos = $condResult['pos'];
        $pos++; // skip comma
        $thenResult = parseExpression($tokens, $pos);
        $pos = $thenResult['pos'];
        $pos++; // skip comma
        $elseResult = parseExpression($tokens, $pos);
        $pos = $elseResult['pos'];
        $pos++; // skip )
        return ['expr' => ['case' => [['when' => $cond, 'then' => $thenResult['expr']]], 'else' => $elseResult['expr']], 'pos' => $pos];
    }

    // Function call or keyword-as-function: NAME(...)
    if (($token['type'] === 'KEYWORD' || $token['type'] === 'IDENTIFIER') &&
        isSQLFunction($token['value']) &&
        sym($tokens, $pos + 1, '(')) {
        return parseFunctionCall($token['value'], $tokens, $pos + 1);
    }

    // Column reference (identifier or keyword used as identifier)
    if ($token['type'] === 'IDENTIFIER' || $token['type'] === 'KEYWORD') {
        // Check for table.column notation
        if (sym($tokens, $pos + 1, '.')) {
            $table = $token['value'];
            $col = $tokens[$pos + 2]['value'] ?? '';
            return ['expr' => "$table.$col", 'pos' => $pos + 3];
        }
        return ['expr' => $token['value'], 'pos' => $pos + 1];
    }

    // Star
    if (sym($tokens, $pos, '*')) {
        return ['expr' => '*', 'pos' => $pos + 1];
    }

    throw new \RuntimeException("Unexpected token '{$token['value']}' ({$token['type']}) at position $pos");
}

/** Parse CASE WHEN w1 THEN t1 [WHEN w2 THEN t2] [ELSE e] END */
function parseCaseExpr(array $tokens, int $pos): array {
    $pos++; // skip CASE
    $branches = [];
    $elseExpr = null;

    while (kw($tokens, $pos, 'WHEN')) {
        $pos++; // skip WHEN
        $condition = parseWhereClause($tokens, $pos);
        $pos = $condition['pos'];
        if (kw($tokens, $pos, 'THEN')) $pos++;
        $thenResult = parseExpression($tokens, $pos);
        $pos = $thenResult['pos'];
        $branches[] = ['when' => $condition['condition'], 'then' => $thenResult['expr']];
    }

    if (kw($tokens, $pos, 'ELSE')) {
        $pos++;
        $elseResult = parseExpression($tokens, $pos);
        $elseExpr = $elseResult['expr'];
        $pos = $elseResult['pos'];
    }

    if (kw($tokens, $pos, 'END')) $pos++;

    $result = ['case' => $branches];
    if ($elseExpr !== null) $result['else'] = $elseExpr;
    return ['expr' => $result, 'pos' => $pos];
}

/** Parse function_name(arg1, arg2, ...) */
function parseFunctionCall(string $fn, array $tokens, int $pos): array {
    $pos = expect($tokens, $pos, 'SYMBOL', '(');
    $args = [];

    // Handle special CAST(expr AS type) syntax
    if (strtoupper($fn) === 'CAST') {
        $exprResult = parseExpression($tokens, $pos);
        $args[] = $exprResult['expr'];
        $pos = $exprResult['pos'];
        if (kw($tokens, $pos, 'AS')) {
            $pos++;
            $typeName = $tokens[$pos]['value'];
            $args[] = ['literal' => normalizeType($typeName)];
            $pos++;
        }
        $pos = expect($tokens, $pos, 'SYMBOL', ')');
        return ['expr' => ['fn' => strtoupper($fn), 'args' => $args], 'pos' => $pos];
    }

    // Handle COUNT(*) specially
    if (strtoupper($fn) === 'COUNT' && sym($tokens, $pos, '*')) {
        $pos++; // skip *
        $pos = expect($tokens, $pos, 'SYMBOL', ')');
        return ['expr' => ['fn' => 'COUNT', 'args' => [['literal' => '*']]], 'pos' => $pos];
    }

    if (!sym($tokens, $pos, ')')) {
        while (true) {
            $argResult = parseExpression($tokens, $pos);
            $args[] = $argResult['expr'];
            $pos = $argResult['pos'];
            if (sym($tokens, $pos, ',')) {
                $pos++;
            } else {
                break;
            }
        }
    }

    $pos = expect($tokens, $pos, 'SYMBOL', ')');
    return ['expr' => ['fn' => strtoupper($fn), 'args' => $args], 'pos' => $pos];
}

/**
 * Detect whether next tokens form a subquery: (SELECT ...)
 * Returns null if not a subquery, otherwise the tokenized subquery event.
 */
function isSubquery(array $tokens, int $pos): bool {
    return sym($tokens, $pos, '(') && kw($tokens, $pos + 1, 'SELECT');
}

/** Parse a subquery (SELECT ...) and return it as a pipeline event */
function parseSubquery(array $tokens, int $pos): array {
    $pos++; // skip (
    // Collect tokens until matching )
    $depth = 1;
    $subTokens = [];
    while ($pos < count($tokens) && $depth > 0) {
        if (sym($tokens, $pos, '(')) $depth++;
        if (sym($tokens, $pos, ')')) {
            $depth--;
            if ($depth === 0) break;
        }
        $subTokens[] = $tokens[$pos];
        $pos++;
    }
    $pos++; // skip closing )
    return ['tokens' => $subTokens, 'pos' => $pos];
}

/** Parse OVER (PARTITION BY ... ORDER BY ...) window specification */
function parseWindowSpec(array $tokens, int $pos): array {
    $spec = ['partitionBy' => null, 'orderBy' => null];
    $pos = expect($tokens, $pos, 'SYMBOL', '(');

    // Empty window: OVER ()
    if (sym($tokens, $pos, ')')) {
        return ['spec' => $spec, 'pos' => $pos + 1];
    }

    // PARTITION BY
    if (kw($tokens, $pos, 'PARTITION') && kw($tokens, $pos + 1, 'BY')) {
        $pos += 2;
        $spec['partitionBy'] = [];
        while ($pos < count($tokens) && !kw($tokens, $pos, 'ORDER') && !sym($tokens, $pos, ')')) {
            $spec['partitionBy'][] = $tokens[$pos]['value'];
            $pos++;
            if (sym($tokens, $pos, ',')) $pos++;
        }
    }

    // ORDER BY
    if (kw($tokens, $pos, 'ORDER') && kw($tokens, $pos + 1, 'BY')) {
        $pos += 2;
        $spec['orderBy'] = [];
        while ($pos < count($tokens) && !sym($tokens, $pos, ')')) {
            $col = $tokens[$pos]['value'];
            $pos++;
            $dir = 'asc';
            if (kw($tokens, $pos, 'ASC')) { $dir = 'asc'; $pos++; }
            elseif (kw($tokens, $pos, 'DESC')) { $dir = 'desc'; $pos++; }
            $spec['orderBy'][] = ['column' => $col, 'direction' => $dir];
            if (sym($tokens, $pos, ',')) $pos++;
        }
    }

    $pos = expect($tokens, $pos, 'SYMBOL', ')');
    return ['spec' => $spec, 'pos' => $pos];
}
