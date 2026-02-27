/**
 * DropTableExecuteGate — drops a table and all its rows, indexes.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';

export class DropTableExecuteGate extends StateGate {
  constructor() { super('drop_table_execute'); }

  reads(event) {
    const table = event.data.table;
    return new ReadSet()
      .ref(`db/tables/${table}/schema`)
      .pattern(`db/tables/${table}/rows/`)
      .pattern(`db/tables/${table}/indexes/`);
  }

  transform(event, state) {
    const { table, ifExists } = event.data;
    const schema = state.refs[`db/tables/${table}/schema`];

    if (schema === null) {
      if (ifExists) {
        return new MutationBatch();
      }
      return new MutationBatch()
        .emit(new Event('error', { message: `Table '${table}' does not exist`, source: 'drop_table_execute' }));
    }

    const batch = new MutationBatch()
      .refDelete(`db/tables/${table}/schema`)
      .refDelete(`db/tables/${table}/next_id`);

    // Delete all row refs
    for (const name of Object.keys(state.patterns[`db/tables/${table}/rows/`])) {
      batch.refDelete(name);
    }

    // Delete all index refs
    for (const name of Object.keys(state.patterns[`db/tables/${table}/indexes/`])) {
      batch.refDelete(name);
    }

    return batch.emit(new Event('table_dropped', { table }));
  }
}
