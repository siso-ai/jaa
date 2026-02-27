<?php
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class SelectParseGate extends PureGate {
    public function __construct() { parent::__construct('select_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 1; // skip SELECT
        $pipeline = [];
        $ctes = $event->data['ctes'] ?? [];
        $recursive = $event->data['recursive'] ?? false;
        $cteColumns = $event->data['cteColumns'] ?? [];

        // DISTINCT
        $distinct = false;
        if (kw($tokens, $pos, 'DISTINCT')) {
            $distinct = true;
            $pos++;
        }

        // Column list
        $colResult = parseColumnList($tokens, $pos);
        $columns = $colResult['columns'];
        $pos = $colResult['pos'];

        // Extract aggregates from columns
        $aggregates = [];
        $projectCols = [];
        $allSelectCols = []; // Tracks all columns in original order for ORDER BY number
        $hasAggregates = false;
        $windowFns = [];

        foreach ($columns as $col) {
            if ($col === '*') {
                $projectCols[] = '*';
                $allSelectCols[] = '*';
            } elseif (is_array($col) && isset($col['aggregate'])) {
                $hasAggregates = true;
                $agg = [
                    'fn' => $col['aggregate']['fn'],
                    'column' => $col['aggregate']['column'],
                    'alias' => $col['alias'],
                ];
                if (!empty($col['aggregate']['distinct'])) $agg['distinct'] = true;
                if (isset($col['aggregate']['separator'])) $agg['separator'] = $col['aggregate']['separator'];
                $aggregates[] = $agg;
                $allSelectCols[] = ['alias' => $col['alias']];
            } elseif (is_array($col) && isset($col['window'])) {
                $windowFns[] = [
                    'fn' => $col['window']['fn'],
                    'column' => $col['window']['column'],
                    'distinct' => $col['window']['distinct'] ?? false,
                    'over' => $col['window']['over'],
                    'alias' => $col['alias'],
                ];
                $allSelectCols[] = ['alias' => $col['alias']];
            } elseif (is_array($col) && isset($col['expr'])) {
                $projectCols[] = $col;
                $allSelectCols[] = $col;
            } else {
                $projectCols[] = $col;
                $allSelectCols[] = $col;
            }
        }

        // FROM (optional — SELECT without FROM returns one row)
        if (!kw($tokens, $pos, 'FROM')) {
            // SELECT without FROM: virtual single-row scan
            $pipeline[] = ['type' => 'virtual_row', 'data' => []];
        } else {
        $pos++;

        // Derived table: FROM (SELECT ...) [AS] alias
        if (isSubquery($tokens, $pos)) {
            $sub = parseSubquery($tokens, $pos);
            $pos = $sub['pos'];
            $alias = 'derived';
            if (kw($tokens, $pos, 'AS')) { $pos++; }
            if (isset($tokens[$pos]) && $tokens[$pos]['type'] === 'IDENTIFIER') {
                $alias = $tokens[$pos]['value'];
                $pos++;
            }
            $pipeline[] = ['type' => 'derived_scan', 'data' => ['subquery' => $sub['tokens'], 'alias' => $alias]];
            $mainAlias = $alias;
        } else {
            $tableRef = parseTableRef($tokens, $pos);
            $mainTable = $tableRef['table'];
            $mainAlias = $tableRef['alias'] ?? $mainTable;
            $pos = $tableRef['pos'];

            // Check if table name is a CTE reference
            if (isset($ctes[$mainTable])) {
                $scanData = ['subquery' => $ctes[$mainTable], 'alias' => $mainAlias];
                if ($recursive && isset($cteColumns[$mainTable])) {
                    $scanData['recursive'] = true;
                    $scanData['cteName'] = $mainTable;
                    $scanData['cteColumns'] = $cteColumns[$mainTable];
                }
                $pipeline[] = ['type' => 'derived_scan', 'data' => $scanData];
            } else {
                $pipeline[] = ['type' => 'table_scan', 'data' => ['table' => $mainTable, 'alias' => $mainAlias]];
            }
        } // end FROM clause
        } // end FROM else

        // JOIN clauses
        while ($pos < count($tokens) && self::isJoinKeyword($tokens, $pos)) {
            $join = self::parseJoin($tokens, $pos, $mainAlias);
            $pipeline[] = $join['step'];
            $pos = $join['pos'];
        }

        // WHERE
        if (kw($tokens, $pos, 'WHERE')) {
            $pos++;
            $whereResult = parseWhereClause($tokens, $pos);
            $pipeline[] = ['type' => 'filter', 'data' => ['where' => $whereResult['condition']]];
            $pos = $whereResult['pos'];
        }

        // GROUP BY
        $groupBy = null;
        if (kw($tokens, $pos, 'GROUP') && kw($tokens, $pos + 1, 'BY')) {
            $pos += 2;
            $groupBy = [];
            while ($pos < count($tokens) && !kw($tokens, $pos, 'HAVING') &&
                   !kw($tokens, $pos, 'ORDER') && !kw($tokens, $pos, 'LIMIT') &&
                   !sym($tokens, $pos, ';')) {
                $col = $tokens[$pos]['value'];
                $pos++;
                // Handle qualified: table.column
                if (sym($tokens, $pos, '.')) {
                    $pos++;
                    $col = $col . '.' . $tokens[$pos]['value'];
                    $pos++;
                }
                $groupBy[] = $col;
                if (sym($tokens, $pos, ',')) $pos++;
            }
        }

        // Aggregates or GROUP BY → add aggregate step
        if ($hasAggregates || $groupBy !== null) {
            $aggList = count($aggregates) > 0
                ? $aggregates
                : [['fn' => 'COUNT', 'column' => '*', 'alias' => 'count']];
            $pipeline[] = ['type' => 'aggregate', 'data' => ['aggregates' => $aggList, 'groupBy' => $groupBy]];
        }

        // HAVING
        if (kw($tokens, $pos, 'HAVING')) {
            $pos++;
            $havingResult = parseWhereClause($tokens, $pos);
            $pipeline[] = ['type' => 'filter', 'data' => ['where' => $havingResult['condition']]];
            $pos = $havingResult['pos'];
        }

        // WINDOW FUNCTIONS — after aggregation so window can see aggregate results
        if (count($windowFns) > 0) {
            $pipeline[] = ['type' => 'window', 'data' => ['windows' => $windowFns]];
            // Add window aliases to project cols so they pass through PROJECT
            foreach ($windowFns as $wf) {
                $projectCols[] = $wf['alias'];
            }
        }

        // PROJECT — add after aggregation so it can reference aliases
        $needsProject = !(count($projectCols) === 1 && $projectCols[0] === '*') &&
                         !(count($projectCols) === 0 && $hasAggregates) &&
                         !$hasAggregates;
        if ($needsProject && count($projectCols) > 0) {
            $pipeline[] = ['type' => 'project', 'data' => ['columns' => $projectCols]];
        }

        // DISTINCT — after projection so it sees only output columns
        if ($distinct) {
            $pipeline[] = ['type' => 'distinct', 'data' => ['columns' => null]];
        }

        // ORDER BY — after PROJECT so it can reference aliases
        if (kw($tokens, $pos, 'ORDER') && kw($tokens, $pos + 1, 'BY')) {
            $pos += 2;
            $orderResult = parseOrderBy($tokens, $pos);
            $pipeline[] = ['type' => 'order_by', 'data' => ['order' => $orderResult['order'], 'selectCols' => $allSelectCols]];
            $pos = $orderResult['pos'];
        }

        // LIMIT / OFFSET
        if (kw($tokens, $pos, 'LIMIT')) {
            $pos++;
            $limit = $tokens[$pos]['value'];
            $pos++;
            $offset = 0;
            if (kw($tokens, $pos, 'OFFSET')) {
                $pos++;
                $offset = $tokens[$pos]['value'];
                $pos++;
            }
            $pipeline[] = ['type' => 'limit', 'data' => ['limit' => $limit, 'offset' => $offset]];
        }

        // UNION / UNION ALL / EXCEPT / INTERSECT
        if (kw($tokens, $pos, 'UNION') || kw($tokens, $pos, 'EXCEPT') || kw($tokens, $pos, 'INTERSECT')) {
            $setOp = $tokens[$pos]['value']; // UNION, EXCEPT, INTERSECT
            $pos++;
            $all = false;
            if (kw($tokens, $pos, 'ALL')) {
                $all = true;
                $pos++;
            }
            // Parse right side as a sub-SELECT (skip the SELECT keyword, re-wrap)
            $rightTokens = array_slice($tokens, $pos);
            // Recursively parse the right side
            $rightEvent = new Event('select_parse', ['tokens' => $rightTokens, 'sql' => '']);
            $rightResult = $this->transform($rightEvent);
            if ($rightResult instanceof Event && $rightResult->type === 'query_plan') {
                $rightPipeline = $rightResult->data['pipeline'];
            } else {
                return $rightResult; // propagate error
            }

            // Hoist ORDER BY / LIMIT from rightmost branch to post-union level
            $postSteps = [];
            $rightLeaf = &$rightPipeline;
            // Walk down to the deepest right pipeline of nested unions
            while (count($rightLeaf) === 1 && $rightLeaf[0]['type'] === 'union') {
                $rightLeaf = &$rightLeaf[0]['data']['right'];
            }
            // Extract trailing order_by, limit, distinct from the leaf
            $hoistTypes = ['order_by', 'limit', 'distinct'];
            while (count($rightLeaf) > 0 && in_array(end($rightLeaf)['type'], $hoistTypes)) {
                $postSteps[] = array_pop($rightLeaf);
            }
            $postSteps = array_reverse($postSteps);

            $finalPipeline = [
                ['type' => 'union', 'data' => [
                    'left' => $pipeline,
                    'right' => $rightPipeline,
                    'setOp' => strtolower($setOp),
                    'all' => $all,
                ]],
            ];
            foreach ($postSteps as $ps) $finalPipeline[] = $ps;

            return new Event('query_plan', ['pipeline' => $finalPipeline, 'ctes' => $ctes]);
        }

        return new Event('query_plan', ['pipeline' => $pipeline, 'ctes' => $ctes]);
    }

    private static function isJoinKeyword(array $tokens, int $pos): bool {
        if (kw($tokens, $pos, 'JOIN')) return true;
        if (kw($tokens, $pos, 'INNER') || kw($tokens, $pos, 'LEFT') ||
            kw($tokens, $pos, 'RIGHT') || kw($tokens, $pos, 'FULL') ||
            kw($tokens, $pos, 'CROSS')) return true;
        return false;
    }

    private static function parseJoin(array $tokens, int $pos, string $leftTable): array {
        $type = 'inner';
        if (kw($tokens, $pos, 'LEFT')) { $type = 'left'; $pos++; if (kw($tokens, $pos, 'OUTER')) $pos++; }
        elseif (kw($tokens, $pos, 'RIGHT')) { $type = 'right'; $pos++; if (kw($tokens, $pos, 'OUTER')) $pos++; }
        elseif (kw($tokens, $pos, 'FULL')) { $type = 'full'; $pos++; if (kw($tokens, $pos, 'OUTER')) $pos++; }
        elseif (kw($tokens, $pos, 'CROSS')) { $type = 'cross'; $pos++; }
        elseif (kw($tokens, $pos, 'INNER')) { $pos++; }

        $pos++; // skip JOIN

        $tableRef = parseTableRef($tokens, $pos);
        $rightAlias = $tableRef['alias'] ?? $tableRef['table'];
        $pos = $tableRef['pos'];

        $on = null;
        if (kw($tokens, $pos, 'ON')) {
            $pos++;
            $conditions = [];
            do {
                // Parse first side: alias.column or column
                $firstAlias = $tokens[$pos]['value'];
                $pos++;
                $firstField = $firstAlias;
                if (sym($tokens, $pos, '.')) {
                    $pos++;
                    $firstField = $tokens[$pos]['value'];
                    $pos++;
                }

                // Support =, <, >, <=, >= operators
                $joinOp = $tokens[$pos]['value'] ?? '=';
                $pos++;

                // Parse second side
                $secondAlias = $tokens[$pos]['value'];
                $pos++;
                $secondField = $secondAlias;
                if (sym($tokens, $pos, '.')) {
                    $pos++;
                    $secondField = $tokens[$pos]['value'];
                    $pos++;
                }

                // Determine which side references the right table
                $cond = null;
                if ($firstAlias === $rightAlias) {
                    $leftRef = ($secondAlias !== $secondField) ? "$secondAlias.$secondField" : $secondField;
                    $cond = ['left' => $leftRef, 'right' => $firstField];
                } elseif ($secondAlias === $rightAlias) {
                    $leftRef = ($firstAlias !== $firstField) ? "$firstAlias.$firstField" : $firstField;
                    $cond = ['left' => $leftRef, 'right' => $secondField];
                } else {
                    $cond = ['left' => $firstField, 'right' => $secondField];
                }
                $conditions[] = $cond;
            } while (kw($tokens, $pos, 'AND') && ++$pos);

            $on = count($conditions) === 1 ? $conditions[0] : $conditions;
        }

        return [
            'step' => [
                'type' => 'join',
                'data' => [
                    'right' => ['table' => $tableRef['table'], 'alias' => $rightAlias],
                    'on' => $on,
                    'type' => $type,
                    'leftAlias' => $leftTable,
                ],
            ],
            'pos' => $pos,
        ];
    }
}
