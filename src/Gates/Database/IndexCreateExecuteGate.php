<?php
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class IndexCreateExecuteGate extends StateGate {
    public function __construct() { parent::__construct('index_create_execute'); }

    public function reads(Event $event): ReadSet {
        $t = $event->data['table'];
        return (new ReadSet())
            ->ref("db/tables/$t/schema")
            ->pattern("db/tables/$t/rows/");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $index = $event->data['index'];
        $column = $event->data['column'];
        $unique = $event->data['unique'] ?? false;
        $schema = $state['refs']["db/tables/$table/schema"];

        if ($schema === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Table '$table' does not exist", 'source' => 'index_create_execute']));
        }

        $colExists = false;
        foreach ($schema['columns'] as $c) {
            if ($c['name'] === $column) { $colExists = true; break; }
        }
        if (!$colExists) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Column '$column' does not exist", 'source' => 'index_create_execute']));
        }

        $rows = array_values($state['patterns']["db/tables/$table/rows/"] ?? []);
        $entries = [];

        foreach ($rows as $row) {
            $key = $row[$column] ?? null;
            $found = false;
            foreach ($entries as &$entry) {
                if ($entry['key'] === $key) {
                    if ($unique) {
                        return (new MutationBatch())
                            ->emit(new Event('error', ['message' => "Duplicate value '$key' for unique index", 'source' => 'index_create_execute']));
                    }
                    $entry['row_ids'][] = $row['id'];
                    $found = true;
                    break;
                }
            }
            unset($entry);
            if (!$found) {
                $entries[] = ['key' => $key, 'row_ids' => [$row['id']]];
            }
        }

        return (new MutationBatch())
            ->put('btree', ['column' => $column, 'unique' => $unique, 'entries' => $entries])
            ->refSet("db/tables/$table/indexes/$index", 0)
            ->emit(new Event('index_created', ['table' => $table, 'index' => $index]));
    }
}
