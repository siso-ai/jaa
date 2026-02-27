/**
 * LimitGate — restricts rows to limit/offset window.
 * PureGate: rows in, sliced rows out. No state access.
 *
 * Exports limitRows() for direct use by QueryPlanGate (Phase 5).
 */
import { PureGate } from '../../protocol/PureGate.js';
import { Event } from '../../core/Event.js';

export function limitRows(rows, limit, offset = 0) {
  if (limit === undefined || limit === null) return rows.slice(offset);
  return rows.slice(offset, offset + limit);
}

export class LimitGate extends PureGate {
  constructor() { super('limit'); }

  transform(event) {
    const limited = limitRows(event.data.rows, event.data.limit, event.data.offset);
    return new Event('limited_result', { rows: limited });
  }
}
