import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { kw, sym, parseIdentList } from './parser-utils.js';

export class ConstraintCreateParseGate extends PureGate {
  constructor() { super('constraint_create_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 2; // skip ALTER TABLE

    const table = tokens[pos].value;
    pos++;

    pos++; // skip ADD
    pos++; // skip CONSTRAINT

    const name = tokens[pos].value;
    pos++;

    // UNIQUE, CHECK, FOREIGN KEY
    let type = 'check';
    let params = {};

    if (kw(tokens, pos, 'UNIQUE')) {
      type = 'unique';
      pos++;
      if (sym(tokens, pos, '(')) {
        const result = parseIdentList(tokens, pos);
        params = { columns: result.idents };
        pos = result.pos;
      }
    } else if (kw(tokens, pos, 'CHECK')) {
      type = 'check';
      pos++;
      // Store raw check expression
      params = { expression: tokens.slice(pos).map(t => t.value).join(' ') };
    } else if (kw(tokens, pos, 'FOREIGN')) {
      type = 'foreign_key';
      pos += 2; // FOREIGN KEY
      if (sym(tokens, pos, '(')) {
        const cols = parseIdentList(tokens, pos);
        params.columns = cols.idents;
        pos = cols.pos;
      }
      if (kw(tokens, pos, 'REFERENCES')) {
        pos++;
        params.refTable = tokens[pos].value;
        pos++;
        if (sym(tokens, pos, '(')) {
          const refCols = parseIdentList(tokens, pos);
          params.refColumns = refCols.idents;
        }
      }
    }

    return new Event('constraint_create_execute', { table, name, type, params });
  }
}

export class ConstraintDropParseGate extends PureGate {
  constructor() { super('constraint_drop_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 2; // skip ALTER TABLE
    const table = tokens[pos].value;
    pos++;
    pos++; // DROP
    pos++; // CONSTRAINT
    const name = tokens[pos].value;
    return new Event('constraint_drop_execute', { table, name });
  }
}
