<?php
/**
 * QueryPlanGate — executes a query pipeline.
 *
 * StateGate: reads tables referenced in scan steps,
 * then runs filter/project/order/limit/distinct/aggregate
 * using the pure functions from the database gate modules.
 *
 * Single event in (query_plan), single event out (query_result).
 */
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

use function Ice\Gates\Database\filterRows;
use function Ice\Gates\Database\projectRows;
use function Ice\Gates\Database\orderByRows;
use function Ice\Gates\Database\limitRows;
use function Ice\Gates\Database\distinctRows;
use function Ice\Gates\Database\aggregateRows;
use function Ice\Gates\Database\joinRows;

class QueryPlanGate extends StateGate {
    public function __construct() { parent::__construct('query_plan'); }

    public function reads(Event $event): ReadSet {
        $rs = new ReadSet();
        $ctes = $event->data['ctes'] ?? [];
        self::scanPipelineReads($event->data['pipeline'], $rs, $ctes);
        return $rs;
    }

    /** Recursively scan a pipeline (including union sub-pipelines) for read dependencies */
    private static function scanPipelineReads(array $pipeline, ReadSet $rs, array $ctes = []): void {
        foreach ($pipeline as $step) {
            if ($step['type'] === 'table_scan') {
                $rs->pattern("db/tables/{$step['data']['table']}/rows/");
            }
            if ($step['type'] === 'index_scan') {
                $rs->ref("db/tables/{$step['data']['table']}/indexes/{$step['data']['index']}");
                $rs->pattern("db/tables/{$step['data']['table']}/rows/");
            }
            if ($step['type'] === 'join') {
                $rightTable = $step['data']['right']['table'];
                if (isset($ctes[$rightTable])) {
                    self::scanSubqueryTokensForReads($ctes[$rightTable], $rs);
                } else {
                    $rs->pattern("db/tables/$rightTable/rows/");
                }
            }
            if ($step['type'] === 'union') {
                self::scanPipelineReads($step['data']['left'], $rs, $ctes);
                self::scanPipelineReads($step['data']['right'], $rs, $ctes);
            }
            // Scan filter conditions and project expressions for subquery table refs
            if ($step['type'] === 'filter') {
                self::scanConditionSubqueries($step['data']['where'] ?? null, $rs);
            }
            if ($step['type'] === 'project') {
                foreach ($step['data']['columns'] ?? [] as $col) {
                    if (is_array($col) && isset($col['expr'])) {
                        self::scanExprSubqueries($col['expr'], $rs);
                    }
                }
            }
            // Derived table subquery
            if ($step['type'] === 'derived_scan') {
                self::scanSubqueryTokensForReads($step['data']['subquery'], $rs);
            }
        }
    }

    /** Scan a condition tree for subquery table references */
    public static function scanConditionSubqueries(?array $cond, ReadSet $rs): void {
        if ($cond === null) return;
        if (isset($cond['and'])) { foreach ($cond['and'] as $c) self::scanConditionSubqueries($c, $rs); return; }
        if (isset($cond['or'])) { foreach ($cond['or'] as $c) self::scanConditionSubqueries($c, $rs); return; }
        if (isset($cond['not'])) { self::scanConditionSubqueries($cond['not'], $rs); return; }
        if (isset($cond['subquery'])) { self::scanSubqueryTokensForReads($cond['subquery'], $rs); }
        if (isset($cond['leftExpr'])) { self::scanExprSubqueries($cond['leftExpr'], $rs); }
        if (isset($cond['rightExpr'])) { self::scanExprSubqueries($cond['rightExpr'], $rs); }
    }

    /** Scan an expression for subquery table references */
    private static function scanExprSubqueries(mixed $expr, ReadSet $rs): void {
        if (!is_array($expr)) return;
        if (isset($expr['subquery'])) { self::scanSubqueryTokensForReads($expr['subquery'], $rs); }
        if (isset($expr['left'])) self::scanExprSubqueries($expr['left'], $rs);
        if (isset($expr['right'])) self::scanExprSubqueries($expr['right'], $rs);
        if (isset($expr['args'])) { foreach ($expr['args'] as $a) self::scanExprSubqueries($a, $rs); }
    }

    /** Extract table names from subquery tokens and add to ReadSet */
    public static function scanSubqueryTokensForReads(array $tokens, ReadSet $rs): void {
        for ($i = 0; $i < count($tokens); $i++) {
            if (($tokens[$i]['value'] ?? '') === 'FROM' && isset($tokens[$i + 1])) {
                $table = $tokens[$i + 1]['value'] ?? '';
                if ($table) $rs->pattern("db/tables/$table/rows/");
            }
        }
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $ctes = $event->data['ctes'] ?? [];
        $rows = self::executePipeline($event->data['pipeline'], $state, $ctes);
        return (new MutationBatch())->emit(new Event('query_result', ['rows' => $rows]));
    }

    /** Execute a pipeline against state, returning row array */
    private static function executePipeline(array $pipeline, array $state, array $ctes = []): array {
        $rows = [];
        $leftTable = ''; // Track current left table for JOIN

        foreach ($pipeline as $step) {
            switch ($step['type']) {
                case 'virtual_row':
                    $rows = [[]]; // Single empty row for SELECT without FROM
                    break;

                case 'table_scan':
                    $pattern = "db/tables/{$step['data']['table']}/rows/";
                    $rows = array_values($state['patterns'][$pattern] ?? []);
                    $leftTable = $step['data']['alias'] ?? $step['data']['table'];
                    break;

                case 'derived_scan':
                    if (!empty($step['data']['recursive'])) {
                        $rows = self::executeRecursiveCTE($step['data'], $state);
                    } else {
                        $rows = self::executeSubquery($step['data']['subquery'], $state);
                    }
                    $leftTable = $step['data']['alias'] ?? 'derived';
                    break;

                case 'window':
                    $rows = self::computeWindowFunctions($rows, $step['data']['windows']);
                    break;

                case 'index_scan':
                    $index = $state['refs']["db/tables/{$step['data']['table']}/indexes/{$step['data']['index']}"] ?? null;
                    if (!$index) break;
                    $matchingIds = [];
                    foreach (($index['entries'] ?? []) as $entry) {
                        if (self::matchesIndexOp($entry['key'], $step['data']['op'], $step['data']['value'])) {
                            foreach ($entry['row_ids'] as $id) $matchingIds[$id] = true;
                        }
                    }
                    $allRows = array_values($state['patterns']["db/tables/{$step['data']['table']}/rows/"] ?? []);
                    $rows = array_values(array_filter($allRows, fn($r) => isset($matchingIds[$r['id']])));
                    break;

                case 'filter':
                    $where = self::resolveConditionSubqueries($step['data']['where'], $state);
                    $rows = filterRows($rows, $where);
                    break;

                case 'project':
                    $cols = self::resolveProjectSubqueries($step['data']['columns'], $state);
                    $rows = projectRows($rows, $cols);
                    break;

                case 'order_by':
                    $order = self::resolveOrderByNumbers($step['data']['order'], $step['data']['selectCols'] ?? []);
                    $rows = orderByRows($rows, $order);
                    break;

                case 'limit':
                    $rows = limitRows($rows, $step['data']['limit'], $step['data']['offset'] ?? 0);
                    break;

                case 'distinct':
                    $rows = distinctRows($rows, $step['data']['columns'] ?? null);
                    break;

                case 'aggregate':
                    $rows = aggregateRows($rows, $step['data']['aggregates'], $step['data']['groupBy'] ?? null);
                    break;

                case 'join':
                    $rightTable = $step['data']['right']['table'];
                    if (isset($ctes[$rightTable])) {
                        $rightRows = self::executeSubquery($ctes[$rightTable], $state);
                    } else {
                        $rightPattern = "db/tables/$rightTable/rows/";
                        $rightRows = array_values($state['patterns'][$rightPattern] ?? []);
                    }
                    $rightAlias = $step['data']['right']['alias'] ?? $rightTable;
                    $leftAlias = $step['data']['leftAlias'] ?? $leftTable;
                    $rows = joinRows($rows, $rightRows, $step['data']['on'], $step['data']['type'] ?? 'inner', $leftAlias, $rightAlias);
                    break;

                case 'union':
                    $leftRows = self::executePipeline($step['data']['left'], $state, $ctes);
                    $rightRows = self::executePipeline($step['data']['right'], $state, $ctes);
                    $setOp = $step['data']['setOp'] ?? 'union';
                    $all = $step['data']['all'] ?? false;

                    if ($setOp === 'union') {
                        $rows = array_merge($leftRows, $rightRows);
                        if (!$all) {
                            $rows = distinctRows($rows, null);
                        }
                    } elseif ($setOp === 'except') {
                        $rightKeys = array_map(fn($r) => json_encode($r), $rightRows);
                        $rows = array_values(array_filter($leftRows, fn($r) => !in_array(json_encode($r), $rightKeys)));
                        if (!$all) $rows = distinctRows($rows, null);
                    } elseif ($setOp === 'intersect') {
                        $rightKeys = array_map(fn($r) => json_encode($r), $rightRows);
                        $rows = array_values(array_filter($leftRows, fn($r) => in_array(json_encode($r), $rightKeys)));
                        if (!$all) $rows = distinctRows($rows, null);
                    }
                    break;
            }
        }

        return $rows;
    }

    private static function matchesIndexOp(mixed $key, string $op, mixed $value): bool {
        return match ($op) {
            'eq'  => $key === $value,
            'neq' => $key !== $value,
            'gt'  => $key > $value,
            'lt'  => $key < $value,
            'gte' => $key >= $value,
            'lte' => $key <= $value,
            default => false,
        };
    }

    // ── Subquery resolution ─────────────────────────────────────

    /** Execute a subquery (tokens) and return result rows */
    /** Resolve numeric column references in ORDER BY (1-based) */
    private static function resolveOrderByNumbers(array $order, array $selectCols): array {
        foreach ($order as &$o) {
            if (is_numeric($o['column'])) {
                $idx = (int)$o['column'] - 1;
                if ($idx >= 0 && $idx < count($selectCols)) {
                    $col = $selectCols[$idx];
                    $o['column'] = is_string($col) ? $col : ($col['alias'] ?? $col['name'] ?? $col['column'] ?? $o['column']);
                }
            }
        }
        unset($o);
        return $order;
    }

    /** Execute a recursive CTE: base UNION ALL recursive */
    private static function executeRecursiveCTE(array $data, array $state): array {
        $tokens = $data['subquery'];
        $cteName = $data['cteName'];
        $cteColumns = $data['cteColumns'];
        $maxIterations = 1000;

        // The subquery tokens should be: base_select UNION ALL recursive_select
        // Find UNION ALL split point
        $unionPos = null;
        $depth = 0;
        for ($i = 0; $i < count($tokens); $i++) {
            if (($tokens[$i]['type'] ?? '') === 'SYMBOL' && $tokens[$i]['value'] === '(') $depth++;
            if (($tokens[$i]['type'] ?? '') === 'SYMBOL' && $tokens[$i]['value'] === ')') $depth--;
            if ($depth === 0 && kw($tokens, $i, 'UNION')) {
                $unionPos = $i;
                break;
            }
        }

        if ($unionPos === null) {
            return self::executeSubquery($tokens, $state);
        }

        $baseTokens = array_slice($tokens, 0, $unionPos);
        $pos = $unionPos + 1; // skip UNION
        if (kw($tokens, $pos, 'ALL')) $pos++;
        $recursiveTokens = array_slice($tokens, $pos);

        // Execute base case
        $allRows = self::executeSubquery($baseTokens, $state);
        // Normalize column names
        $allRows = array_map(function($row) use ($cteColumns) {
            $vals = array_values($row);
            $out = [];
            foreach ($cteColumns as $i => $col) {
                $out[$col] = $vals[$i] ?? null;
            }
            return $out;
        }, $allRows);

        $currentRows = $allRows;
        for ($iter = 0; $iter < $maxIterations && count($currentRows) > 0; $iter++) {
            // Create virtual state with CTE table
            $virtualState = $state;
            $virtualPattern = "db/tables/$cteName/rows/";
            $virtualState['patterns'][$virtualPattern] = [];
            foreach ($currentRows as $i => $row) {
                $virtualState['patterns'][$virtualPattern]["db/tables/$cteName/rows/$i"] = array_merge(['__rowid__' => $i + 1], $row);
            }
            $virtualState['refs']["db/tables/$cteName/schema"] = ['columns' => array_map(fn($c) => ['name' => $c, 'type' => 'text'], $cteColumns)];

            $newRows = self::executeSubquery($recursiveTokens, $virtualState);
            $newRows = array_map(function($row) use ($cteColumns) {
                $vals = array_values($row);
                $out = [];
                foreach ($cteColumns as $i => $col) {
                    $out[$col] = $vals[$i] ?? null;
                }
                return $out;
            }, $newRows);

            if (count($newRows) === 0) break;
            $allRows = array_merge($allRows, $newRows);
            $currentRows = $newRows;
        }

        return $allRows;
    }

    public static function executeSubquery(array $tokens, array $state): array {
        $parseGate = new SelectParseGate();
        $result = $parseGate->transform(new \Ice\Core\Event('select_parse', ['tokens' => $tokens, 'sql' => '']));
        if (!$result || $result->type !== 'query_plan') return [];
        return self::executePipeline($result->data['pipeline'], $state);
    }

    /** Resolve subqueries in a condition tree, replacing with concrete values */
    public static function resolveConditionSubqueries(?array $cond, array $state): ?array {
        if ($cond === null) return null;

        if (isset($cond['and'])) {
            return ['and' => array_map(fn($c) => self::resolveConditionSubqueries($c, $state), $cond['and'])];
        }
        if (isset($cond['or'])) {
            return ['or' => array_map(fn($c) => self::resolveConditionSubqueries($c, $state), $cond['or'])];
        }
        if (isset($cond['not'])) {
            return ['not' => self::resolveConditionSubqueries($cond['not'], $state)];
        }

        // EXISTS subquery
        if (isset($cond['exists']) && isset($cond['subquery'])) {
            $rows = self::executeSubquery($cond['subquery'], $state);
            return ['exists' => true, 'resolved' => count($rows) > 0];
        }

        // IN subquery
        if (isset($cond['subquery']) && ($cond['op'] ?? '') === 'in') {
            $rows = self::executeSubquery($cond['subquery'], $state);
            $values = [];
            foreach ($rows as $row) {
                $vals = array_values($row);
                $values[] = $vals[0] ?? null; // First column only
            }
            $resolved = $cond;
            unset($resolved['subquery']);
            $resolved['value'] = $values;
            return $resolved;
        }

        // Scalar subquery in leftExpr/rightExpr
        if (isset($cond['leftExpr'])) {
            $cond['leftExpr'] = self::resolveExprSubqueries($cond['leftExpr'], $state);
        }
        if (isset($cond['rightExpr'])) {
            $cond['rightExpr'] = self::resolveExprSubqueries($cond['rightExpr'], $state);
        }

        return $cond;
    }

    /** Resolve subqueries in an expression, replacing with scalar values */
    private static function resolveExprSubqueries(mixed $expr, array $state): mixed {
        if (!is_array($expr)) return $expr;

        if (isset($expr['subquery']) && !array_key_exists('resolved', $expr)) {
            $rows = self::executeSubquery($expr['subquery'], $state);
            if (count($rows) > 0) {
                $vals = array_values($rows[0]);
                return array_merge($expr, ['resolved' => $vals[0] ?? null]);
            }
            return array_merge($expr, ['resolved' => null]);
        }

        if (isset($expr['left'])) $expr['left'] = self::resolveExprSubqueries($expr['left'], $state);
        if (isset($expr['right'])) $expr['right'] = self::resolveExprSubqueries($expr['right'], $state);
        if (isset($expr['args'])) {
            $expr['args'] = array_map(fn($a) => self::resolveExprSubqueries($a, $state), $expr['args']);
        }
        if (isset($expr['case'])) {
            foreach ($expr['case'] as &$branch) {
                $branch['when'] = self::resolveConditionSubqueries($branch['when'], $state);
                $branch['then'] = self::resolveExprSubqueries($branch['then'], $state);
            }
            unset($branch);
        }
        if (isset($expr['else'])) $expr['else'] = self::resolveExprSubqueries($expr['else'], $state);

        return $expr;
    }

    /** Resolve subqueries in project column list */
    private static function resolveProjectSubqueries(array $columns, array $state): array {
        foreach ($columns as &$col) {
            if (is_array($col) && isset($col['expr'])) {
                $col['expr'] = self::resolveExprSubqueries($col['expr'], $state);
            }
        }
        unset($col);
        return $columns;
    }

    /** Compute window functions and add result columns to each row */
    private static function computeWindowFunctions(array $rows, array $windows): array {
        if (count($rows) === 0) return $rows;

        foreach ($windows as $win) {
            $fn = strtoupper($win['fn']);
            $col = $win['column'] ?? '*';
            $alias = $win['alias'];
            $partitionBy = $win['over']['partitionBy'] ?? null;
            $orderBy = $win['over']['orderBy'] ?? null;

            // Group rows into partitions
            $partitions = [];
            foreach ($rows as $idx => $row) {
                $key = $partitionBy
                    ? json_encode(array_map(fn($c) => $row[$c] ?? null, $partitionBy))
                    : '__all__';
                $partitions[$key][] = $idx;
            }

            foreach ($partitions as $indices) {
                // Sort within partition if ORDER BY specified
                if ($orderBy) {
                    usort($indices, function ($a, $b) use ($rows, $orderBy) {
                        foreach ($orderBy as $spec) {
                            $va = $rows[$a][$spec['column']] ?? null;
                            $vb = $rows[$b][$spec['column']] ?? null;
                            $dir = ($spec['direction'] ?? 'asc') === 'desc' ? -1 : 1;
                            if ($va === null && $vb === null) continue;
                            if ($va === null) return 1;
                            if ($vb === null) return -1;
                            if ($va < $vb) return -1 * $dir;
                            if ($va > $vb) return 1 * $dir;
                        }
                        return 0;
                    });
                }

                // Compute values
                $partitionRows = array_map(fn($i) => $rows[$i], $indices);
                $rank = 0;
                $denseRank = 0;
                $prevVals = null;
                $sameCount = 0;

                foreach ($indices as $posInPart => $rowIdx) {
                    switch ($fn) {
                        case 'ROW_NUMBER':
                            $rows[$rowIdx][$alias] = $posInPart + 1;
                            break;

                        case 'RANK':
                            if ($orderBy) {
                                $curVals = array_map(fn($s) => $rows[$rowIdx][$s['column']] ?? null, $orderBy);
                                if ($prevVals === null || $curVals !== $prevVals) {
                                    $rank = $posInPart + 1;
                                    $prevVals = $curVals;
                                }
                                $rows[$rowIdx][$alias] = $rank;
                            } else {
                                $rows[$rowIdx][$alias] = 1;
                            }
                            break;

                        case 'DENSE_RANK':
                            if ($orderBy) {
                                $curVals = array_map(fn($s) => $rows[$rowIdx][$s['column']] ?? null, $orderBy);
                                if ($prevVals === null || $curVals !== $prevVals) {
                                    $denseRank++;
                                    $prevVals = $curVals;
                                }
                                $rows[$rowIdx][$alias] = $denseRank;
                            } else {
                                $rows[$rowIdx][$alias] = 1;
                            }
                            break;

                        case 'SUM':
                            $vals = array_filter(array_map(fn($r) => $r[$col] ?? null, $partitionRows), fn($v) => $v !== null && is_numeric($v));
                            $rows[$rowIdx][$alias] = array_sum($vals);
                            break;

                        case 'AVG':
                            $vals = array_filter(array_map(fn($r) => $r[$col] ?? null, $partitionRows), fn($v) => $v !== null && is_numeric($v));
                            $rows[$rowIdx][$alias] = count($vals) > 0 ? array_sum($vals) / count($vals) : null;
                            break;

                        case 'COUNT':
                            $rows[$rowIdx][$alias] = $col === '*'
                                ? count($partitionRows)
                                : count(array_filter($partitionRows, fn($r) => ($r[$col] ?? null) !== null));
                            break;

                        case 'MIN':
                            $vals = array_filter(array_map(fn($r) => $r[$col] ?? null, $partitionRows), fn($v) => $v !== null);
                            $rows[$rowIdx][$alias] = count($vals) > 0 ? min($vals) : null;
                            break;

                        case 'MAX':
                            $vals = array_filter(array_map(fn($r) => $r[$col] ?? null, $partitionRows), fn($v) => $v !== null);
                            $rows[$rowIdx][$alias] = count($vals) > 0 ? max($vals) : null;
                            break;
                    }
                }
            }
        }

        return $rows;
    }
}
