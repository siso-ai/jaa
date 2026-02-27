<?php
namespace Ice\Gates\Database;

function evaluateCondition(?array $condition, array $row): bool {
    if ($condition === null) return true;

    if (isset($condition['and'])) {
        foreach ($condition['and'] as $c) {
            if (!evaluateCondition($c, $row)) return false;
        }
        return true;
    }
    if (isset($condition['or'])) {
        foreach ($condition['or'] as $c) {
            if (evaluateCondition($c, $row)) return true;
        }
        return false;
    }
    if (isset($condition['not'])) {
        return !evaluateCondition($condition['not'], $row);
    }
    if (isset($condition['exists'])) {
        // exists is pre-resolved to a boolean by QueryPlanGate
        return (bool)($condition['resolved'] ?? false);
    }

    // Expression-based comparison: {leftExpr, op, rightExpr}
    if (isset($condition['leftExpr'])) {
        $val = evaluateExpression($condition['leftExpr'], $row);
        $op = $condition['op'] ?? null;

        // IS NULL / IS NOT NULL with expression
        if ($op === 'is_null') return $val === null;
        if ($op === 'is_not_null') return $val !== null;

        // IN with subquery not evaluated here (handled at query plan level)
        if ($op === 'in') {
            $target = $condition['value'] ?? [];
            return is_array($target) && in_array($val, $target, true);
        }
        if ($op === 'like') {
            return matchLike($val, $condition['value'] ?? '', false);
        }
        if ($op === 'ilike') {
            return matchLike($val, $condition['value'] ?? '', true);
        }

        $right = isset($condition['rightExpr']) ? evaluateExpression($condition['rightExpr'], $row) : null;

        return match ($op) {
            '=', '==' => $val === $right,
            '!=', '<>' => $val !== $right,
            '<' => $val < $right,
            '>' => $val > $right,
            '<=' => $val <= $right,
            '>=' => $val >= $right,
            default => false,
        };
    }

    // Classic column-based comparison: {column, op, value}
    $colName = $condition['column'];
    $val = $row[$colName] ?? null;
    // table.column or alias.column fallback
    if ($val === null && !array_key_exists($colName, $row) && strpos($colName, '.') !== false) {
        $shortCol = substr($colName, strpos($colName, '.') + 1);
        $val = $row[$shortCol] ?? null;
    }
    $target = $condition['value'] ?? null;

    return match ($condition['op'] ?? null) {
        '=', '==' => $val === $target,
        '!=', '<>' => $val !== $target,
        '<' => $val < $target,
        '>' => $val > $target,
        '<=' => $val <= $target,
        '>=' => $val >= $target,
        'in' => is_array($target) && in_array($val, $target, true),
        'like' => matchLike($val, $target, false),
        'ilike' => matchLike($val, $target, true),
        'is_null' => $val === null,
        'is_not_null' => $val !== null,
        default => false,
    };
}

function evaluateExpression(mixed $expr, array $row): mixed {
    if (is_string($expr)) {
        // Direct column name
        if (array_key_exists($expr, $row)) return $row[$expr];
        // table.column or alias.column — try stripping prefix
        $dotPos = strpos($expr, '.');
        if ($dotPos !== false) {
            $col = substr($expr, $dotPos + 1);
            if (array_key_exists($col, $row)) return $row[$col];
        }
        return null;
    }
    if (is_int($expr) || is_float($expr) || is_bool($expr) || $expr === null) return $expr;

    if (is_array($expr)) {
        if (array_key_exists('literal', $expr)) return $expr['literal'];

        // Pre-resolved scalar subquery
        if (isset($expr['subquery']) && array_key_exists('resolved', $expr)) {
            return $expr['resolved'];
        }

        // Arithmetic
        if (isset($expr['op']) && array_key_exists('left', $expr) && array_key_exists('right', $expr)) {
            $left = evaluateExpression($expr['left'], $row);
            $right = evaluateExpression($expr['right'], $row);
            return match ($expr['op']) {
                '+' => $left + $right,
                '-' => $left - $right,
                '*' => $left * $right,
                '/' => $right != 0 ? $left / $right : null,
                '%' => $right != 0 ? $left % $right : null,
                default => null,
            };
        }

        // Function calls
        if (isset($expr['fn'])) {
            $fnUpper = strtoupper($expr['fn']);
            // For aggregate functions, check if pre-computed in row (HAVING support)
            $aggFns = ['SUM', 'COUNT', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT'];
            if (in_array($fnUpper, $aggFns)) {
                $argStr = isset($expr['args'][0]) ? (is_string($expr['args'][0]) ? $expr['args'][0] : '*') : '*';
                $synKey = $fnUpper . '(' . $argStr . ')';
                if (array_key_exists($synKey, $row)) return $row[$synKey];
            }
            $args = array_map(fn($a) => evaluateExpression($a, $row), $expr['args'] ?? []);
            return match (strtoupper($expr['fn'])) {
                // String functions
                'UPPER' => is_string($args[0] ?? null) ? strtoupper($args[0]) : null,
                'LOWER' => is_string($args[0] ?? null) ? strtolower($args[0]) : null,
                'LENGTH', 'CHAR_LENGTH', 'CHARACTER_LENGTH' => is_string($args[0] ?? null) ? strlen($args[0]) : null,
                'CONCAT' => implode('', array_map(fn($a) => $a ?? '', $args)),
                'SUBSTR', 'SUBSTRING' => is_string($args[0] ?? null)
                    ? substr($args[0], ($args[1] ?? 1) - 1, $args[2] ?? null)
                    : null,
                'REPLACE' => is_string($args[0] ?? null) && is_string($args[1] ?? null)
                    ? str_replace($args[1], $args[2] ?? '', $args[0])
                    : null,
                'TRIM' => is_string($args[0] ?? null) ? trim($args[0]) : null,
                'LTRIM' => is_string($args[0] ?? null) ? ltrim($args[0]) : null,
                'RTRIM' => is_string($args[0] ?? null) ? rtrim($args[0]) : null,
                'LEFT' => is_string($args[0] ?? null) && is_numeric($args[1] ?? null)
                    ? substr($args[0], 0, (int)$args[1]) : null,
                'RIGHT' => is_string($args[0] ?? null) && is_numeric($args[1] ?? null)
                    ? substr($args[0], -(int)$args[1]) : null,
                'REVERSE' => is_string($args[0] ?? null) ? strrev($args[0]) : null,
                'REPEAT' => is_string($args[0] ?? null) && is_numeric($args[1] ?? null)
                    ? str_repeat($args[0], max(0, (int)$args[1])) : null,
                'LPAD' => is_string($args[0] ?? null) && is_numeric($args[1] ?? null)
                    ? str_pad($args[0], (int)$args[1], $args[2] ?? ' ', STR_PAD_LEFT) : null,
                'RPAD' => is_string($args[0] ?? null) && is_numeric($args[1] ?? null)
                    ? str_pad($args[0], (int)$args[1], $args[2] ?? ' ', STR_PAD_RIGHT) : null,
                'POSITION', 'INSTR' => is_string($args[0] ?? null) && is_string($args[1] ?? null)
                    ? (($p = strpos($args[0], $args[1])) !== false ? $p + 1 : 0) : 0,
                'STARTS_WITH' => is_string($args[0] ?? null) && is_string($args[1] ?? null)
                    ? (str_starts_with($args[0], $args[1]) ? 1 : 0) : 0,
                'ENDS_WITH' => is_string($args[0] ?? null) && is_string($args[1] ?? null)
                    ? (str_ends_with($args[0], $args[1]) ? 1 : 0) : 0,

                // Math functions
                'ABS' => is_numeric($args[0] ?? null) ? abs($args[0]) : null,
                'ROUND' => is_numeric($args[0] ?? null)
                    ? round($args[0], isset($args[1]) ? (int)$args[1] : 0)
                    : null,
                'CEIL', 'CEILING' => is_numeric($args[0] ?? null) ? (int)ceil($args[0]) : null,
                'FLOOR' => is_numeric($args[0] ?? null) ? (int)floor($args[0]) : null,
                'POWER', 'POW' => is_numeric($args[0] ?? null) && is_numeric($args[1] ?? null)
                    ? pow($args[0], $args[1]) : null,
                'SQRT' => is_numeric($args[0] ?? null) && $args[0] >= 0 ? sqrt($args[0]) : null,
                'MOD' => is_numeric($args[0] ?? null) && is_numeric($args[1] ?? null) && $args[1] != 0
                    ? fmod($args[0], $args[1]) : null,
                'SIGN' => is_numeric($args[0] ?? null) ? ($args[0] > 0 ? 1 : ($args[0] < 0 ? -1 : 0)) : null,
                'LOG' => is_numeric($args[0] ?? null) && $args[0] > 0
                    ? (isset($args[1]) ? log($args[0]) / log($args[1]) : log10($args[0])) : null,
                'LN' => is_numeric($args[0] ?? null) && $args[0] > 0 ? log($args[0]) : null,
                'EXP' => is_numeric($args[0] ?? null) ? exp($args[0]) : null,
                'PI' => M_PI,
                'RANDOM', 'RAND' => mt_rand() / mt_getrandmax(),

                // Null/type functions
                'IFNULL' => ($args[0] ?? null) !== null ? $args[0] : ($args[1] ?? null),
                'NULLIF' => ($args[0] ?? null) === ($args[1] ?? null) ? null : $args[0],
                'COALESCE' => coalesceArgs($args),
                'CAST' => castValue($args[0] ?? null, $args[1] ?? 'text'),
                'TYPEOF' => gettype($args[0] ?? null) === 'integer' ? 'integer'
                    : (gettype($args[0] ?? null) === 'double' ? 'real'
                    : (is_string($args[0] ?? null) ? 'text'
                    : ($args[0] === null ? 'null' : 'text'))),
                'GREATEST' => count($args) > 0 ? max(array_filter($args, fn($a) => $a !== null)) : null,
                'LEAST' => count($args) > 0 ? min(array_filter($args, fn($a) => $a !== null)) : null,
                'IIF' => ($args[0] ?? null) ? ($args[1] ?? null) : ($args[2] ?? null),
                'IFNULL' => ($args[0] ?? null) !== null ? $args[0] : ($args[1] ?? null),
                // Date/time functions
                'DATE' => (function() use ($args) {
                    $input = $args[0] ?? 'now';
                    if (strtolower($input) === 'now') return date('Y-m-d');
                    return date('Y-m-d', strtotime($input));
                })(),
                'TIME' => (function() use ($args) {
                    $input = $args[0] ?? 'now';
                    if (strtolower($input) === 'now') return date('H:i:s');
                    return date('H:i:s', strtotime($input));
                })(),
                'DATETIME' => (function() use ($args) {
                    $input = $args[0] ?? 'now';
                    if (strtolower($input) === 'now') return date('Y-m-d H:i:s');
                    return date('Y-m-d H:i:s', strtotime($input));
                })(),
                'STRFTIME' => (function() use ($args) {
                    $fmt = $args[0] ?? '%Y-%m-%d';
                    $input = $args[1] ?? 'now';
                    $ts = strtolower($input) === 'now' ? time() : strtotime($input);
                    // Convert SQL strftime format to PHP date format
                    $map = ['%Y' => 'Y', '%m' => 'm', '%d' => 'd', '%H' => 'H', '%M' => 'i', '%S' => 's', '%w' => 'w', '%j' => 'z', '%W' => 'W'];
                    $phpFmt = str_replace(array_keys($map), array_values($map), $fmt);
                    return date($phpFmt, $ts);
                })(),
                'NOW' => date('Y-m-d H:i:s'),
                'CURRENT_TIMESTAMP' => date('Y-m-d H:i:s'),
                'CURRENT_DATE' => date('Y-m-d'),
                'CURRENT_TIME' => date('H:i:s'),
                default => null,
            };
        }

        // COALESCE shorthand
        if (isset($expr['coalesce'])) {
            foreach ($expr['coalesce'] as $e) {
                $val = evaluateExpression($e, $row);
                if ($val !== null) return $val;
            }
            return null;
        }

        // CASE WHEN
        if (isset($expr['case'])) {
            foreach ($expr['case'] as $branch) {
                if (evaluateCondition($branch['when'], $row)) {
                    return evaluateExpression($branch['then'], $row);
                }
            }
            return isset($expr['else']) ? evaluateExpression($expr['else'], $row) : null;
        }
    }

    return null;
}

function matchLike(mixed $value, mixed $pattern, bool $caseInsensitive = false): bool {
    if ($value === null || !is_string($pattern)) return false;
    $value = (string)$value;

    $regex = '/^';
    for ($i = 0; $i < strlen($pattern); $i++) {
        $ch = $pattern[$i];
        if ($ch === '%') $regex .= '.*';
        elseif ($ch === '_') $regex .= '.';
        else $regex .= preg_quote($ch, '/');
    }
    $regex .= '$/' . ($caseInsensitive ? 'i' : 's');
    return (bool)preg_match($regex, $value);
}

function coalesceArgs(array $args): mixed {
    foreach ($args as $a) {
        if ($a !== null) return $a;
    }
    return null;
}

function castValue(mixed $val, mixed $type): mixed {
    if ($val === null) return null;
    $type = is_string($type) ? strtolower($type) : 'text';
    return match ($type) {
        'integer', 'int' => (int)$val,
        'real', 'float', 'double' => (float)$val,
        'text', 'string', 'varchar' => (string)$val,
        'boolean', 'bool' => (bool)$val,
        default => $val,
    };
}
