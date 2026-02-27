/**
 * DistinctGate — removes duplicate rows.
 * PureGate: rows in, deduplicated rows out. No state access.
 *
 * Exports distinctRows() for direct use by QueryPlanGate (Phase 5).
 */
import { PureGate } from '../../protocol/PureGate.js';
import { Event } from '../../core/Event.js';

export function distinctRows(rows, columns) {
  const seen = new Set();
  return rows.filter(row => {
    const key = columns && columns.length > 0
      ? JSON.stringify(columns.map(c => row[c]))
      : JSON.stringify(row);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

export class DistinctGate extends PureGate {
  constructor() { super('distinct'); }

  transform(event) {
    const unique = distinctRows(event.data.rows, event.data.columns);
    return new Event('distinct_result', { rows: unique });
  }
}
