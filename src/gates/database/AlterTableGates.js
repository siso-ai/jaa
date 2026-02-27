/**
 * AlterTableGates — ALTER TABLE ADD COLUMN, DROP COLUMN, RENAME TABLE.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';

export class AlterTableAddColumnGate extends StateGate {
  constructor() { super('alter_table_add_column'); }

  reads(event) {
    const t = event.data.table;
    return new ReadSet()
      .ref(`db/tables/${t}/schema`)
      .pattern(`db/tables/${t}/rows/`);
  }

  transform(event, state) {
    const { table, column } = event.data;
    const schema = state.refs[`db/tables/${table}/schema`];

    if (schema === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Table '${table}' does not exist`, source: 'alter_table_add_column' }));
    }

    // Check duplicate
    if (schema.columns.some(c => c.name === column.name)) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Column '${column.name}' already exists`, source: 'alter_table_add_column' }));
    }

    const newSchema = {
      ...schema,
      columns: [...schema.columns, {
        name: column.name,
        type: column.type || 'text',
        nullable: column.nullable !== undefined ? column.nullable : true,
        default: column.default !== undefined ? column.default : null,
      }],
    };

    const batch = new MutationBatch()
      .put('schema', newSchema)
      .refSet(`db/tables/${table}/schema`, 0);

    // Backfill existing rows
    const defaultVal = column.default !== undefined ? column.default : null;
    const rows = state.patterns[`db/tables/${table}/rows/`] || {};
    let putIdx = 1;
    for (const [refName, row] of Object.entries(rows)) {
      const updatedRow = { ...row, [column.name]: defaultVal };
      batch.put('row', updatedRow);
      batch.refSet(refName, putIdx++);
    }

    return batch.emit(new Event('column_added', { table, column: column.name }));
  }
}

export class AlterTableDropColumnGate extends StateGate {
  constructor() { super('alter_table_drop_column'); }

  reads(event) {
    const t = event.data.table;
    return new ReadSet()
      .ref(`db/tables/${t}/schema`)
      .pattern(`db/tables/${t}/rows/`);
  }

  transform(event, state) {
    const { table, column } = event.data;
    const schema = state.refs[`db/tables/${table}/schema`];

    if (schema === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Table '${table}' does not exist`, source: 'alter_table_drop_column' }));
    }

    if (!schema.columns.some(c => c.name === column)) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Column '${column}' does not exist`, source: 'alter_table_drop_column' }));
    }

    if (column === 'id') {
      return new MutationBatch()
        .emit(new Event('error', { message: "Cannot drop 'id' column", source: 'alter_table_drop_column' }));
    }

    const newSchema = {
      ...schema,
      columns: schema.columns.filter(c => c.name !== column),
    };

    const batch = new MutationBatch()
      .put('schema', newSchema)
      .refSet(`db/tables/${table}/schema`, 0);

    // Remove column from rows
    const rows = state.patterns[`db/tables/${table}/rows/`] || {};
    let putIdx = 1;
    for (const [refName, row] of Object.entries(rows)) {
      const { [column]: _, ...rest } = row;
      batch.put('row', rest);
      batch.refSet(refName, putIdx++);
    }

    return batch.emit(new Event('column_dropped', { table, column }));
  }
}

export class RenameTableGate extends StateGate {
  constructor() { super('rename_table'); }

  reads(event) {
    const { table: oldT, newName: newT } = event.data;
    return new ReadSet()
      .ref(`db/tables/${oldT}/schema`)
      .ref(`db/tables/${oldT}/next_id`)
      .ref(`db/tables/${newT}/schema`)
      .pattern(`db/tables/${oldT}/rows/`)
      .pattern(`db/tables/${oldT}/indexes/`);
  }

  transform(event, state) {
    const { table: oldTable, newName: newTable } = event.data;
    const schema = state.refs[`db/tables/${oldTable}/schema`];

    if (schema === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Table '${oldTable}' does not exist`, source: 'rename_table' }));
    }

    if (state.refs[`db/tables/${newTable}/schema`] !== null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Table '${newTable}' already exists`, source: 'rename_table' }));
    }

    const newSchema = { ...schema, name: newTable };
    const batch = new MutationBatch();

    batch.put('schema', newSchema);
    batch.refSet(`db/tables/${newTable}/schema`, 0);

    const nextId = state.refs[`db/tables/${oldTable}/next_id`];
    let putIdx = 1;
    if (nextId !== null) {
      batch.put('counter', nextId);
      batch.refSet(`db/tables/${newTable}/next_id`, putIdx++);
    }

    batch.refDelete(`db/tables/${oldTable}/schema`);
    batch.refDelete(`db/tables/${oldTable}/next_id`);

    // Copy rows
    const rows = state.patterns[`db/tables/${oldTable}/rows/`] || {};
    for (const [refName, row] of Object.entries(rows)) {
      const rowId = row.id || refName.split('/').pop();
      batch.put('row', row);
      batch.refSet(`db/tables/${newTable}/rows/${rowId}`, putIdx++);
      batch.refDelete(refName);
    }

    // Copy indexes
    const indexes = state.patterns[`db/tables/${oldTable}/indexes/`] || {};
    for (const [refName, index] of Object.entries(indexes)) {
      const idxName = refName.split('/').pop();
      batch.put('btree', index);
      batch.refSet(`db/tables/${newTable}/indexes/${idxName}`, putIdx++);
      batch.refDelete(refName);
    }

    return batch.emit(new Event('table_renamed', { oldName: oldTable, newName: newTable }));
  }
}
