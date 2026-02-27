/**
 * IndexCreateExecuteGate — creates an index on a table column.
 * Scans all rows to build the initial index.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';

export class IndexCreateExecuteGate extends StateGate {
  constructor() { super('index_create_execute'); }

  reads(event) {
    const table = event.data.table;
    return new ReadSet()
      .ref(`db/tables/${table}/schema`)
      .pattern(`db/tables/${table}/rows/`);
  }

  transform(event, state) {
    const { table, index, column, unique } = event.data;
    const schema = state.refs[`db/tables/${table}/schema`];

    if (schema === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Table '${table}' does not exist`, source: 'index_create_execute' }));
    }

    // Verify column exists
    if (!schema.columns.some(c => c.name === column)) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Column '${column}' does not exist`, source: 'index_create_execute' }));
    }

    // Build index from existing rows
    const rows = Object.values(state.patterns[`db/tables/${table}/rows/`] || {});
    const entries = [];

    for (const row of rows) {
      const key = row[column];
      const existing = entries.find(e => e.key === key);
      if (existing) {
        if (unique) {
          return new MutationBatch()
            .emit(new Event('error', { message: `Duplicate value '${key}' for unique index`, source: 'index_create_execute' }));
        }
        existing.row_ids.push(row.id);
      } else {
        entries.push({ key, row_ids: [row.id] });
      }
    }

    const btree = { column, unique: !!unique, entries };

    return new MutationBatch()
      .put('btree', btree)
      .refSet(`db/tables/${table}/indexes/${index}`, 0)
      .emit(new Event('index_created', { table, index }));
  }
}
