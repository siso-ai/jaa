import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { kw, sym, parseWhereClause } from './parser-utils.js';

export class DeleteParseGate extends PureGate {
  constructor() { super('delete_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 1; // skip DELETE

    // FROM
    if (kw(tokens, pos, 'FROM')) pos++;

    const table = tokens[pos].value;
    pos++;

    let where = null;
    if (kw(tokens, pos, 'WHERE')) {
      pos++;
      const result = parseWhereClause(tokens, pos);
      where = result.condition;
      pos = result.pos;
    }

    const data = { table, where };

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

    return new Event('delete_execute', data);
  }
}
