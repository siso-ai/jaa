/**
 * TableScanGate — reads all rows from a table.
 * StateGate: reads row refs, returns rows as event.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';

export class TableScanGate extends StateGate {
  constructor() { super('table_scan'); }

  reads(event) {
    return new ReadSet()
      .pattern(`db/tables/${event.data.table}/rows/`);
  }

  transform(event, state) {
    const rows = Object.values(
      state.patterns[`db/tables/${event.data.table}/rows/`] || {}
    );
    return new MutationBatch()
      .emit(new Event('scan_result', { table: event.data.table, rows }));
  }
}
