import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { kw, sym, parseValueList, parseIdentList, parseExpression } from './parser-utils.js';
import { evaluateExpression } from '../../database/expression.js';

export class InsertParseGate extends PureGate {
  constructor() { super('insert_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 1; // skip INSERT

    // INTO
    if (kw(tokens, pos, 'INTO')) pos++;

    const table = tokens[pos].value;
    pos++;

    // Optional column list
    let columns = null;
    if (sym(tokens, pos, '(')) {
      const result = parseIdentList(tokens, pos);
      columns = result.idents;
      pos = result.pos;
    }

    // VALUES, SELECT, or DEFAULT VALUES
    if (kw(tokens, pos, 'DEFAULT') && kw(tokens, pos + 1, 'VALUES')) {
      return new Event('insert_execute', { table, row: {} });
    }

    if (kw(tokens, pos, 'SELECT')) {
      const selectTokens = tokens.slice(pos);
      return new Event('insert_select', { table, columns, selectTokens });
    }

    if (kw(tokens, pos, 'VALUES')) pos++;

    const allRows = [];
    while (pos < tokens.length && sym(tokens, pos, '(')) {
      const result = parseValueList(tokens, pos);
      const rowValues = [...result.values];
      // Evaluate expressions in values
      if (result.exprs) {
        for (let i = 0; i < result.exprs.length; i++) {
          const expr = result.exprs[i];
          if (typeof expr === 'object' && expr !== null && !('literal' in expr)) {
            rowValues[i] = evaluateExpression(expr, {});
          }
        }
      }
      allRows.push(rowValues);
      pos = result.pos;
      if (sym(tokens, pos, ',')) pos++;
    }

    // ON CONFLICT handling (UPSERT)
    let onConflict = null;
    if (kw(tokens, pos, 'ON') && kw(tokens, pos + 1, 'CONFLICT')) {
      pos += 2;
      onConflict = { action: 'nothing', column: null, updates: {} };
      if (sym(tokens, pos, '(')) {
        pos++;
        onConflict.column = tokens[pos].value;
        pos++;
        pos++; // skip )
      }
      if (kw(tokens, pos, 'DO')) pos++;
      if (kw(tokens, pos, 'NOTHING')) {
        onConflict.action = 'nothing';
        pos++;
      } else if (kw(tokens, pos, 'UPDATE')) {
        pos++;
        if (kw(tokens, pos, 'SET')) pos++;
        onConflict.action = 'update';
        onConflict.updates = {};
        while (pos < tokens.length && !sym(tokens, pos, ';') && !kw(tokens, pos, 'RETURNING')) {
          const col = tokens[pos].value;
          pos++;
          pos++; // skip =
          const exprResult = parseExpression(tokens, pos);
          pos = exprResult.pos;
          onConflict.updates[col] = exprResult.expr;
          if (sym(tokens, pos, ',')) pos++;
        }
      }
    }

    // RETURNING
    let returning = null;
    if (kw(tokens, pos, 'RETURNING')) {
      pos++;
      if (sym(tokens, pos, '*')) {
        returning = ['*'];
        pos++;
      } else {
        returning = [];
        while (pos < tokens.length && !sym(tokens, pos, ';')) {
          returning.push(tokens[pos].value);
          pos++;
          if (sym(tokens, pos, ',')) pos++;
        }
      }
    }

    if (allRows.length === 1) {
      const data = { table, row: buildRow(columns, allRows[0]) };
      if (onConflict) data.onConflict = onConflict;
      if (returning) data.returning = returning;
      return new Event('insert_execute', data);
    }

    return allRows.map(values => {
      const data = { table, row: buildRow(columns, values) };
      if (onConflict) data.onConflict = onConflict;
      if (returning) data.returning = returning;
      return new Event('insert_execute', data);
    });
  }
}

function buildRow(columns, values) {
  const row = {};
  if (columns) {
    for (let i = 0; i < columns.length; i++) {
      if (values[i] !== undefined) {
        row[columns[i]] = values[i];
      }
    }
  } else {
    // Without columns, use positional naming (execute gate resolves against schema)
    for (let i = 0; i < values.length; i++) {
      row[`_col${i}`] = values[i];
    }
  }
  return row;
}
