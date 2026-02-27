/**
 * IndexScanGate — reads rows using an index.
 * Supports eq, range, gt, lt, gte, lte operations.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';

export class IndexScanGate extends StateGate {
  constructor() { super('index_scan'); }

  reads(event) {
    const table = event.data.table;
    return new ReadSet()
      .ref(`db/tables/${table}/indexes/${event.data.index}`)
      .pattern(`db/tables/${table}/rows/`);
  }

  transform(event, state) {
    const { table, index: indexName, op, value, low, high } = event.data;
    const index = state.refs[`db/tables/${table}/indexes/${indexName}`];

    if (index === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Index '${indexName}' not found`, source: 'index_scan' }));
    }

    // Find matching row_ids from index
    const matchingIds = new Set();
    for (const entry of (index.entries || [])) {
      if (matchesOp(entry.key, op, value, low, high)) {
        for (const id of entry.row_ids) {
          matchingIds.add(id);
        }
      }
    }

    // Resolve rows
    const allRows = state.patterns[`db/tables/${table}/rows/`] || {};
    const rows = Object.values(allRows).filter(r => matchingIds.has(r.id));

    return new MutationBatch()
      .emit(new Event('scan_result', { table, rows }));
  }
}

function matchesOp(key, op, value, low, high) {
  switch (op) {
    case 'eq':  return key === value;
    case 'neq': return key !== value;
    case 'gt':  return key > value;
    case 'lt':  return key < value;
    case 'gte': return key >= value;
    case 'lte': return key <= value;
    case 'range': return key >= low && key <= high;
    default: return false;
  }
}
