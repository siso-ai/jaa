/**
 * CreateTableExecuteGate — creates a table in the store.
 * Stores schema and initializes row counter.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';

export class CreateTableExecuteGate extends StateGate {
  constructor() { super('create_table_execute'); }

  reads(event) {
    return new ReadSet()
      .ref(`db/tables/${event.data.table}/schema`);
  }

  transform(event, state) {
    const { table, columns, ifNotExists } = event.data;
    const existing = state.refs[`db/tables/${table}/schema`];

    if (existing !== null) {
      if (ifNotExists) {
        return new MutationBatch()
          .emit(new Event('table_exists', { table }));
      }
      return new MutationBatch()
        .emit(new Event('error', { message: `Table '${table}' already exists`, source: 'create_table_execute' }));
    }

    const schema = { name: table, columns: columns || [] };

    return new MutationBatch()
      .put('schema', schema)
      .refSet(`db/tables/${table}/schema`, 0)
      .put('counter', '0')
      .refSet(`db/tables/${table}/next_id`, 1)
      .emit(new Event('table_created', { table }));
  }
}
