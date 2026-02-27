import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { kw, sym, parseWhereClause, parseExpression } from './parser-utils.js';

export class UpdateParseGate extends PureGate {
  constructor() { super('update_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 1; // skip UPDATE

    const table = tokens[pos].value;
    pos++;

    // SET
    pos++; // skip SET

    const changes = {};
    const changesExprs = {};
    while (pos < tokens.length && !kw(tokens, pos, 'WHERE') && !kw(tokens, pos, 'RETURNING') && !kw(tokens, pos, 'FROM') && !sym(tokens, pos, ';')) {
      const column = tokens[pos].value;
      pos++;
      pos++; // skip =
      const exprResult = parseExpression(tokens, pos);
      const expr = exprResult.expr;
      pos = exprResult.pos;

      // Simple literal → backward-compatible changes format
      if (typeof expr === 'object' && expr !== null && expr.literal !== undefined) {
        changes[column] = expr.literal;
      } else {
        changesExprs[column] = expr;
      }
      if (sym(tokens, pos, ',')) pos++;
    }

    // FROM clause (PostgreSQL-style join update)
    let fromTable = null;
    let fromAlias = null;
    if (kw(tokens, pos, 'FROM')) {
      pos++;
      fromTable = tokens[pos].value;
      pos++;
      fromAlias = fromTable;
      if (tokens[pos] && tokens[pos].type === 'IDENTIFIER' &&
          !kw(tokens, pos, 'WHERE') && !kw(tokens, pos, 'RETURNING')) {
        if (kw(tokens, pos, 'AS')) pos++;
        fromAlias = tokens[pos].value;
        pos++;
      }
    }

    let where = null;
    if (kw(tokens, pos, 'WHERE')) {
      pos++;
      const result = parseWhereClause(tokens, pos);
      where = result.condition;
      pos = result.pos;
    }

    const data = { table, changes, where };
    if (Object.keys(changesExprs).length > 0) {
      data.changesExprs = changesExprs;
    }
    if (fromTable) {
      data.fromTable = fromTable;
      data.fromAlias = fromAlias;
    }

    // RETURNING
    if (kw(tokens, pos, 'RETURNING')) {
      pos++;
      if (sym(tokens, pos, '*')) {
        data.returning = ['*'];
      } else {
        data.returning = [];
        while (pos < tokens.length && !sym(tokens, pos, ';')) {
          data.returning.push(tokens[pos].value);
          pos++;
          if (sym(tokens, pos, ',')) pos++;
        }
      }
    }

    return new Event('update_execute', data);
  }
}
