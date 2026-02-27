/**
 * OrderByGate — sorts rows by columns and directions.
 * PureGate: rows in, sorted rows out. No state access.
 *
 * Exports orderByRows() for direct use by QueryPlanGate (Phase 5).
 */
import { PureGate } from '../../protocol/PureGate.js';
import { Event } from '../../core/Event.js';

export function orderByRows(rows, order) {
  if (!order || order.length === 0) return rows;

  return [...rows].sort((a, b) => {
    for (const spec of order) {
      const { column, direction } = spec;
      const dir = (direction || 'asc').toLowerCase() === 'desc' ? -1 : 1;
      const nullsFirst = spec.nulls ? spec.nulls === 'first' : false;

      let va = a[column];
      if (va === undefined && column.indexOf('.') !== -1) {
        va = a[column.substring(column.indexOf('.') + 1)];
      }
      let vb = b[column];
      if (vb === undefined && column.indexOf('.') !== -1) {
        vb = b[column.substring(column.indexOf('.') + 1)];
      }

      const aNull = (va === null || va === undefined);
      const bNull = (vb === null || vb === undefined);
      if (aNull && bNull) continue;
      if (aNull) return nullsFirst ? -1 : 1;
      if (bNull) return nullsFirst ? 1 : -1;

      if (va < vb) return -1 * dir;
      if (va > vb) return 1 * dir;
    }
    return 0;
  });
}

export class OrderByGate extends PureGate {
  constructor() { super('order_by'); }

  transform(event) {
    const sorted = orderByRows(event.data.rows, event.data.order);
    return new Event('ordered_result', { rows: sorted });
  }
}
