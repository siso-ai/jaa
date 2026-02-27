/**
 * FilterGate — filters rows by a where condition.
 * PureGate: rows in, filtered rows out. No state access.
 *
 * Exports filterRows() for direct use by QueryPlanGate (Phase 5).
 */
import { PureGate } from '../../protocol/PureGate.js';
import { Event } from '../../core/Event.js';
import { evaluateCondition } from './expression.js';

export function filterRows(rows, where) {
  if (!where) return rows;
  return rows.filter(row => evaluateCondition(where, row));
}

export class FilterGate extends PureGate {
  constructor() { super('filter'); }

  transform(event) {
    const filtered = filterRows(event.data.rows, event.data.where);
    return new Event('filter_result', { rows: filtered });
  }
}
