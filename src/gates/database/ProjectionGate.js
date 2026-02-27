/**
 * ProjectionGate — selects specific columns from rows.
 * PureGate: rows in, projected rows out. No state access.
 *
 * Exports projectRows() for direct use by QueryPlanGate (Phase 5).
 */
import { PureGate } from '../../protocol/PureGate.js';
import { Event } from '../../core/Event.js';
import { evaluateExpression } from './expression.js';

export function projectRows(rows, columns) {
  if (!columns || columns.length === 0) return rows;
  if (columns.length === 1 && columns[0] === '*') return rows;

  return rows.map(row => {
    const projected = {};
    for (const col of columns) {
      if (typeof col === 'string') {
        // Resolve alias.column → column
        let key = col;
        if (!(key in row) && key.indexOf('.') !== -1) {
          key = key.substring(key.indexOf('.') + 1);
        }
        const outputName = col.indexOf('.') !== -1 ? col.substring(col.indexOf('.') + 1) : col;
        projected[outputName] = row[key] !== undefined ? row[key] : null;
      } else if (col.expr !== undefined && col.alias) {
        projected[col.alias] = evaluateExpression(col.expr, row);
      }
    }
    return projected;
  });
}

export class ProjectionGate extends PureGate {
  constructor() { super('project'); }

  transform(event) {
    const projected = projectRows(event.data.rows, event.data.columns);
    return new Event('project_result', { rows: projected });
  }
}
