<?php
/**
 * AlterTableGates — ALTER TABLE ADD COLUMN, DROP COLUMN, RENAME TABLE.
 */
namespace Ice\Gates\Database;

use Ice\Protocol\StateGate;
use Ice\Protocol\ReadSet;
use Ice\Protocol\MutationBatch;
use Ice\Core\Event;

class AlterTableAddColumnGate extends StateGate {
    public function __construct() { parent::__construct('alter_table_add_column'); }

    public function reads(Event $event): ReadSet {
        $t = $event->data['table'];
        return (new ReadSet())
            ->ref("db/tables/$t/schema")
            ->pattern("db/tables/$t/rows/");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $column = $event->data['column']; // ['name' => ..., 'type' => ..., 'default' => ...]
        $schema = $state['refs']["db/tables/$table/schema"];

        if ($schema === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Table '$table' does not exist", 'source' => 'alter_table_add_column']));
        }

        // Check for duplicate column
        foreach ($schema['columns'] as $col) {
            if ($col['name'] === $column['name']) {
                return (new MutationBatch())
                    ->emit(new Event('error', ['message' => "Column '{$column['name']}' already exists", 'source' => 'alter_table_add_column']));
            }
        }

        // Add column to schema
        $newSchema = $schema;
        $newSchema['columns'][] = [
            'name' => $column['name'],
            'type' => $column['type'] ?? 'text',
            'nullable' => $column['nullable'] ?? true,
            'default' => $column['default'] ?? null,
        ];

        $batch = (new MutationBatch())
            ->put('schema', $newSchema)
            ->refSet("db/tables/$table/schema", 0);

        // Backfill existing rows with default value
        $defaultVal = $column['default'] ?? null;
        $rows = $state['patterns']["db/tables/$table/rows/"] ?? [];
        $putIdx = 1;
        foreach ($rows as $refName => $row) {
            $updatedRow = $row;
            $updatedRow[$column['name']] = $defaultVal;
            $batch->put('row', $updatedRow);
            $batch->refSet($refName, $putIdx++);
        }

        return $batch->emit(new Event('column_added', [
            'table' => $table, 'column' => $column['name'],
        ]));
    }
}

class AlterTableDropColumnGate extends StateGate {
    public function __construct() { parent::__construct('alter_table_drop_column'); }

    public function reads(Event $event): ReadSet {
        $t = $event->data['table'];
        return (new ReadSet())
            ->ref("db/tables/$t/schema")
            ->pattern("db/tables/$t/rows/");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $table = $event->data['table'];
        $colName = $event->data['column'];
        $schema = $state['refs']["db/tables/$table/schema"];

        if ($schema === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Table '$table' does not exist", 'source' => 'alter_table_drop_column']));
        }

        // Check column exists
        $found = false;
        $newCols = [];
        foreach ($schema['columns'] as $col) {
            if ($col['name'] === $colName) {
                $found = true;
            } else {
                $newCols[] = $col;
            }
        }

        if (!$found) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Column '$colName' does not exist", 'source' => 'alter_table_drop_column']));
        }

        // Don't allow dropping 'id'
        if ($colName === 'id') {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Cannot drop 'id' column", 'source' => 'alter_table_drop_column']));
        }

        $newSchema = $schema;
        $newSchema['columns'] = $newCols;

        $batch = (new MutationBatch())
            ->put('schema', $newSchema)
            ->refSet("db/tables/$table/schema", 0);

        // Remove column from existing rows
        $rows = $state['patterns']["db/tables/$table/rows/"] ?? [];
        $putIdx = 1;
        foreach ($rows as $refName => $row) {
            $updatedRow = $row;
            unset($updatedRow[$colName]);
            $batch->put('row', $updatedRow);
            $batch->refSet($refName, $putIdx++);
        }

        return $batch->emit(new Event('column_dropped', [
            'table' => $table, 'column' => $colName,
        ]));
    }
}

class RenameTableGate extends StateGate {
    public function __construct() { parent::__construct('rename_table'); }

    public function reads(Event $event): ReadSet {
        $oldT = $event->data['table'];
        $newT = $event->data['newName'];
        return (new ReadSet())
            ->ref("db/tables/$oldT/schema")
            ->ref("db/tables/$oldT/next_id")
            ->ref("db/tables/$newT/schema")
            ->pattern("db/tables/$oldT/rows/")
            ->pattern("db/tables/$oldT/indexes/");
    }

    public function transformEvent(Event $event, array $state): MutationBatch {
        $oldTable = $event->data['table'];
        $newTable = $event->data['newName'];

        $schema = $state['refs']["db/tables/$oldTable/schema"];
        if ($schema === null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Table '$oldTable' does not exist", 'source' => 'rename_table']));
        }

        if ($state['refs']["db/tables/$newTable/schema"] !== null) {
            return (new MutationBatch())
                ->emit(new Event('error', ['message' => "Table '$newTable' already exists", 'source' => 'rename_table']));
        }

        // Update schema table name
        $newSchema = $schema;
        $newSchema['name'] = $newTable;

        $batch = new MutationBatch();

        // Put schema under new name
        $batch->put('schema', $newSchema);
        $batch->refSet("db/tables/$newTable/schema", 0);

        // Copy next_id
        $nextId = $state['refs']["db/tables/$oldTable/next_id"];
        if ($nextId !== null) {
            $batch->put('counter', $nextId);
            $batch->refSet("db/tables/$newTable/next_id", 1);
        }

        // Delete old refs
        $batch->refDelete("db/tables/$oldTable/schema");
        $batch->refDelete("db/tables/$oldTable/next_id");

        // Copy rows to new table prefix
        $putIdx = $nextId !== null ? 2 : 1;
        $rows = $state['patterns']["db/tables/$oldTable/rows/"] ?? [];
        foreach ($rows as $refName => $row) {
            $rowId = $row['id'] ?? basename($refName);
            $batch->put('row', $row);
            $batch->refSet("db/tables/$newTable/rows/$rowId", $putIdx++);
            $batch->refDelete($refName);
        }

        // Copy indexes
        $indexes = $state['patterns']["db/tables/$oldTable/indexes/"] ?? [];
        foreach ($indexes as $refName => $index) {
            $idxName = basename($refName);
            $batch->put('btree', $index);
            $batch->refSet("db/tables/$newTable/indexes/$idxName", $putIdx++);
            $batch->refDelete($refName);
        }

        return $batch->emit(new Event('table_renamed', [
            'oldName' => $oldTable, 'newName' => $newTable,
        ]));
    }
}
