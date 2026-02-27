<?php
namespace Ice\Gates\Database;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

function projectRows(array $rows, ?array $columns): array {
    if ($columns === null || count($columns) === 0) return $rows;
    if (count($columns) === 1 && $columns[0] === '*') return $rows;

    return array_map(function ($row) use ($columns) {
        $projected = [];
        foreach ($columns as $col) {
            if (is_string($col)) {
                // Resolve alias.column → column
                $key = $col;
                if (!array_key_exists($key, $row) && strpos($key, '.') !== false) {
                    $key = substr($key, strpos($key, '.') + 1);
                }
                $outputName = strpos($col, '.') !== false ? substr($col, strpos($col, '.') + 1) : $col;
                $projected[$outputName] = $row[$key] ?? null;
            } elseif (is_array($col) && isset($col['expr'], $col['alias'])) {
                $projected[$col['alias']] = evaluateExpression($col['expr'], $row);
            }
        }
        return $projected;
    }, $rows);
}

class ProjectionGate extends PureGate {
    public function __construct() { parent::__construct('project'); }

    public function transform(Event $event): Event|array|null {
        $projected = projectRows($event->data['rows'], $event->data['columns'] ?? null);
        return new Event('project_result', ['rows' => $projected]);
    }
}
