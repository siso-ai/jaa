/**
 * AlterTableParseGates — parse ALTER TABLE ADD COLUMN, DROP COLUMN, RENAME.
 */
import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { kw, sym, parseLiteralValue, normalizeType } from './parser-utils.js';

export class AlterTableAddColumnParseGate extends PureGate {
  constructor() { super('alter_table_add_column_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 2; // skip ALTER TABLE

    const table = tokens[pos].value;
    pos++;

    pos++; // skip ADD
    if (kw(tokens, pos, 'COLUMN')) pos++;

    const colName = tokens[pos].value;
    pos++;

    let type = 'text';
    const typeKeywords = ['INTEGER','INT','TEXT','VARCHAR','REAL','FLOAT','DOUBLE','BOOLEAN','BOOL',
      'BLOB','DATE','TIMESTAMP','BIGINT','SMALLINT','NUMERIC','DECIMAL','CHAR','STRING'];
    if (pos < tokens.length && tokens[pos].type === 'KEYWORD' && typeKeywords.includes(tokens[pos].value)) {
      type = normalizeType(tokens[pos].value);
      pos++;
      if (sym(tokens, pos, '(')) {
        while (!sym(tokens, pos, ')') && pos < tokens.length) pos++;
        pos++;
      }
    }

    let nullable = true;
    let defaultVal = null;

    while (pos < tokens.length && !sym(tokens, pos, ';')) {
      if (kw(tokens, pos, 'NOT') && kw(tokens, pos + 1, 'NULL')) {
        nullable = false;
        pos += 2;
      } else if (kw(tokens, pos, 'DEFAULT')) {
        pos++;
        const lit = parseLiteralValue(tokens, pos);
        defaultVal = lit.value;
        pos = lit.pos;
      } else {
        pos++;
      }
    }

    return new Event('alter_table_add_column', {
      table,
      column: { name: colName, type, nullable, default: defaultVal },
    });
  }
}

export class AlterTableDropColumnParseGate extends PureGate {
  constructor() { super('alter_table_drop_column_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 2; // skip ALTER TABLE

    const table = tokens[pos].value;
    pos++;

    pos++; // skip DROP
    if (kw(tokens, pos, 'COLUMN')) pos++;

    const column = tokens[pos].value;

    return new Event('alter_table_drop_column', { table, column });
  }
}

export class RenameTableParseGate extends PureGate {
  constructor() { super('rename_table_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 2; // skip ALTER TABLE

    const table = tokens[pos].value;
    pos++;

    pos++; // skip RENAME
    if (kw(tokens, pos, 'TO')) pos++;

    const newName = tokens[pos].value;

    return new Event('rename_table', { table, newName });
  }
}
