<?php
namespace Ice\Gates\Query\SQL;

use Ice\Protocol\PureGate;
use Ice\Core\Event;

class InsertParseGate extends PureGate {
    public function __construct() { parent::__construct('insert_parse'); }

    public function transform(Event $event): Event|array|null {
        $tokens = $event->data['tokens'];
        $pos = 1; // skip INSERT

        // INTO
        if (kw($tokens, $pos, 'INTO')) $pos++;

        $table = $tokens[$pos]['value'];
        $pos++;

        // Optional column list
        $columns = null;
        if (sym($tokens, $pos, '(')) {
            $result = parseIdentList($tokens, $pos);
            $columns = $result['idents'];
            $pos = $result['pos'];
        }

        // VALUES, SELECT, or DEFAULT VALUES
        if (kw($tokens, $pos, 'DEFAULT') && kw($tokens, $pos + 1, 'VALUES')) {
            return new Event('insert_execute', ['table' => $table, 'row' => []]);
        }

        if (kw($tokens, $pos, 'SELECT')) {
            // INSERT...SELECT
            $selectTokens = array_slice($tokens, $pos);
            return new Event('insert_select', [
                'table' => $table,
                'columns' => $columns,
                'selectTokens' => $selectTokens,
            ]);
        }

        if (kw($tokens, $pos, 'VALUES')) $pos++;

        // Parse value rows
        $allRows = [];
        $allExprs = [];
        while ($pos < count($tokens) && sym($tokens, $pos, '(')) {
            $result = parseValueList($tokens, $pos);
            $allRows[] = $result['values'];
            if (isset($result['exprs'])) $allExprs[] = $result['exprs'];
            $pos = $result['pos'];
            if (sym($tokens, $pos, ',')) $pos++;
        }

        // Evaluate expressions in values
        if (count($allExprs) > 0) {
            foreach ($allExprs as $ri => $rowExprs) {
                foreach ($rowExprs as $ci => $expr) {
                    if (is_array($expr) && !isset($expr['literal'])) {
                        $allRows[$ri][$ci] = \Ice\Gates\Database\evaluateExpression($expr, []);
                    }
                }
            }
        }

        $row = self::buildRow($columns, $allRows[0] ?? []);

        // ON CONFLICT handling (UPSERT)
        $onConflict = null;
        if (kw($tokens, $pos, 'ON') && kw($tokens, $pos + 1, 'CONFLICT')) {
            $pos += 2;
            $onConflict = ['action' => 'nothing', 'column' => null, 'updates' => []];
            // (column)
            if (sym($tokens, $pos, '(')) {
                $pos++;
                $onConflict['column'] = $tokens[$pos]['value'];
                $pos++;
                $pos++; // skip )
            }
            // DO UPDATE SET ... | DO NOTHING
            if (kw($tokens, $pos, 'DO')) $pos++;
            if (kw($tokens, $pos, 'NOTHING')) {
                $onConflict['action'] = 'nothing';
                $pos++;
            } elseif (kw($tokens, $pos, 'UPDATE')) {
                $pos++;
                if (kw($tokens, $pos, 'SET')) $pos++;
                $onConflict['action'] = 'update';
                $onConflict['updates'] = [];
                while ($pos < count($tokens) && !sym($tokens, $pos, ';') && !kw($tokens, $pos, 'RETURNING')) {
                    $col = $tokens[$pos]['value'];
                    $pos++;
                    $pos++; // skip =
                    $exprResult = parseExpression($tokens, $pos);
                    $pos = $exprResult['pos'];
                    $onConflict['updates'][$col] = $exprResult['expr'];
                    if (sym($tokens, $pos, ',')) $pos++;
                }
            }
        }

        // RETURNING clause
        $returning = null;
        if (kw($tokens, $pos, 'RETURNING')) {
            $pos++;
            if (sym($tokens, $pos, '*')) {
                $returning = ['*'];
                $pos++;
            } else {
                $returning = [];
                while ($pos < count($tokens) && !sym($tokens, $pos, ';')) {
                    $returning[] = $tokens[$pos]['value'];
                    $pos++;
                    if (sym($tokens, $pos, ',')) $pos++;
                }
            }
        }

        // Multi-row: PureGate can return an array of events
        if (count($allRows) > 1) {
            $events = [];
            foreach ($allRows as $values) {
                $data = ['table' => $table, 'row' => self::buildRow($columns, $values)];
                if ($onConflict) $data['onConflict'] = $onConflict;
                if ($returning) $data['returning'] = $returning;
                $events[] = new Event('insert_execute', $data);
            }
            return $events;
        }

        $data = ['table' => $table, 'row' => $row];
        if ($onConflict) $data['onConflict'] = $onConflict;
        if ($returning) $data['returning'] = $returning;
        return new Event('insert_execute', $data);
    }

    private static function buildRow(?array $columns, array $values): array {
        $row = [];
        if ($columns !== null) {
            for ($i = 0; $i < count($columns); $i++) {
                if (isset($values[$i]) || array_key_exists($i, $values)) {
                    $row[$columns[$i]] = $values[$i];
                }
            }
        } else {
            for ($i = 0; $i < count($values); $i++) {
                $row["_col{$i}"] = $values[$i];
            }
        }
        return $row;
    }
}
