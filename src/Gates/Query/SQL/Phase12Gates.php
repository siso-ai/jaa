<?php
/**
 * Phase 12 Gates — EXPLAIN, INSERT...SELECT, CREATE TABLE AS SELECT.
 */
namespace Ice\Gates\Query\SQL;

use Ice\Core\Event;
use Ice\Protocol\PureGate;

/**
 * ExplainGate — parses the inner SQL as a SELECT and returns the query plan
 * as a result rather than executing it.
 */
class ExplainGate extends PureGate {
    public function __construct() { parent::__construct('explain'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        if (count($tokens) === 0 || $tokens[0]['value'] !== 'SELECT') {
            return new Event('error', ['message' => 'EXPLAIN only supports SELECT', 'source' => 'explain']);
        }
        // Re-parse as SELECT
        $selectEvent = new Event('select_parse', ['tokens' => $tokens, 'sql' => $event->data['sql'] ?? '']);
        $parseGate = new SelectParseGate();
        $result = $parseGate->transform($selectEvent);
        if ($result === null || $result->type !== 'query_plan') {
            return $result;
        }
        // Return pipeline description as query_result
        $steps = self::describePipeline($result->data['pipeline']);
        return new Event('query_result', ['rows' => $steps]);
    }

    private static function describePipeline(array $pipeline, int $depth = 0): array {
        $rows = [];
        foreach ($pipeline as $i => $step) {
            $prefix = str_repeat('  ', $depth);
            $desc = match ($step['type']) {
                'table_scan' => "SCAN {$step['data']['table']}",
                'index_scan' => "INDEX SCAN {$step['data']['table']}.{$step['data']['index']}",
                'filter' => 'FILTER',
                'project' => 'PROJECT',
                'order_by' => 'ORDER BY',
                'limit' => "LIMIT {$step['data']['limit']}" . (($step['data']['offset'] ?? 0) ? " OFFSET {$step['data']['offset']}" : ''),
                'distinct' => 'DISTINCT',
                'aggregate' => 'AGGREGATE',
                'join' => strtoupper($step['data']['type'] ?? 'inner') . " JOIN {$step['data']['right']['table']}",
                'union' => strtoupper($step['data']['setOp'] ?? 'union') . (($step['data']['all'] ?? false) ? ' ALL' : ''),
                default => strtoupper($step['type']),
            };
            $rows[] = ['step' => $i + 1, 'operation' => $prefix . $desc];
            if ($step['type'] === 'union') {
                $rows = array_merge($rows,
                    [['step' => null, 'operation' => $prefix . '  LEFT:']],
                    self::describePipeline($step['data']['left'], $depth + 2),
                    [['step' => null, 'operation' => $prefix . '  RIGHT:']],
                    self::describePipeline($step['data']['right'], $depth + 2),
                );
            }
        }
        return $rows;
    }
}

/**
 * InsertSelectGate — handles INSERT INTO table [(cols)] SELECT ...
 * PureGate: transforms into insert_select_plan event for the StateGate.
 */
class InsertSelectGate extends PureGate {
    public function __construct() { parent::__construct('insert_select'); }

    public function transform(Event $event): Event|array|null {
        $table = $event->data['table'];
        $columns = $event->data['columns'] ?? null;
        $selectTokens = $event->data['selectTokens'];

        // Parse the SELECT
        $parseGate = new SelectParseGate();
        $selectEvent = new Event('select_parse', ['tokens' => $selectTokens, 'sql' => '']);
        $result = $parseGate->transform($selectEvent);

        if ($result === null || $result->type !== 'query_plan') {
            return $result ?? new Event('error', ['message' => 'Invalid SELECT in INSERT...SELECT', 'source' => 'insert_select']);
        }

        return new Event('insert_select_plan', [
            'table' => $table,
            'columns' => $columns,
            'pipeline' => $result->data['pipeline'],
        ]);
    }
}

/**
 * CreateTableAsSelectGate — handles CREATE TABLE ... AS SELECT ...
 * PureGate: transforms into insert_select_plan with createTable flag.
 */
class CreateTableAsSelectGate extends PureGate {
    public function __construct() { parent::__construct('create_table_as_select'); }

    public function transform(Event $event): Event|array|null {
        $table = $event->data['table'];
        $ifNotExists = $event->data['ifNotExists'] ?? false;
        $selectTokens = $event->data['selectTokens'];

        // Parse the SELECT
        $parseGate = new SelectParseGate();
        $selectEvent = new Event('select_parse', ['tokens' => $selectTokens, 'sql' => '']);
        $result = $parseGate->transform($selectEvent);

        if ($result === null || $result->type !== 'query_plan') {
            return $result ?? new Event('error', ['message' => 'Invalid SELECT in CREATE TABLE AS', 'source' => 'create_table_as_select']);
        }

        return new Event('insert_select_plan', [
            'table' => $table,
            'columns' => null,
            'pipeline' => $result->data['pipeline'],
            'createTable' => true,
            'ifNotExists' => $ifNotExists,
        ]);
    }
}
