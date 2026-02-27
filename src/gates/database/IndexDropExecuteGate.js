/**
 * IndexDropExecuteGate — drops an index.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';

export class IndexDropExecuteGate extends StateGate {
  constructor() { super('index_drop_execute'); }

  reads(event) {
    return new ReadSet()
      .ref(`db/tables/${event.data.table}/indexes/${event.data.index}`);
  }

  transform(event, state) {
    const { table, index } = event.data;
    const existing = state.refs[`db/tables/${table}/indexes/${index}`];

    if (existing === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Index '${index}' does not exist`, source: 'index_drop_execute' }));
    }

    return new MutationBatch()
      .refDelete(`db/tables/${table}/indexes/${index}`)
      .emit(new Event('index_dropped', { table, index }));
  }
}
