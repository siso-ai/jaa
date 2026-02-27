import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { kw, sym, match, normalizeType, parseLiteralValue } from './parser-utils.js';

export class CreateTableParseGate extends PureGate {
  constructor() { super('create_table_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 0;

    // CREATE TABLE [IF NOT EXISTS] name
    pos++; // CREATE
    pos++; // TABLE
    let ifNotExists = false;
    if (kw(tokens, pos, 'IF')) { pos += 3; ifNotExists = true; } // IF NOT EXISTS

    const table = tokens[pos].value;
    pos++;

    // CREATE TABLE ... AS SELECT ...
    if (kw(tokens, pos, 'AS')) {
      pos++; // skip AS
      const selectTokens = tokens.slice(pos);
      return new Event('create_table_as_select', { table, selectTokens, ifNotExists });
    }

    // ( column_defs )
    pos++; // (
    const columns = [];

    while (!sym(tokens, pos, ')') && pos < tokens.length) {
      // Skip table-level PRIMARY KEY(...)
      if (kw(tokens, pos, 'PRIMARY')) {
        while (pos < tokens.length && !sym(tokens, pos, ')')) pos++;
        if (sym(tokens, pos, ')') && tokens[pos + 1]?.type !== 'SYMBOL') break;
        // if nested, skip the inner close paren
        pos++;
        if (sym(tokens, pos, ',')) pos++;
        continue;
      }

      const colName = tokens[pos].value;
      pos++;

      let type = 'text';
      if (pos < tokens.length && tokens[pos].type === 'KEYWORD' && ['INTEGER', 'INT', 'TEXT', 'VARCHAR', 'REAL', 'FLOAT', 'DOUBLE', 'BOOLEAN', 'BOOL', 'BLOB', 'DATE', 'TIMESTAMP', 'BIGINT', 'SMALLINT', 'NUMERIC', 'DECIMAL', 'CHAR', 'STRING'].includes(tokens[pos].value)) {
        type = normalizeType(tokens[pos].value);
        pos++;
        // Skip length specifiers: VARCHAR(255)
        if (sym(tokens, pos, '(')) {
          while (!sym(tokens, pos, ')') && pos < tokens.length) pos++;
          pos++; // skip )
        }
      }

      let nullable = true;
      let defaultVal = null;

      // Parse column constraints
      while (pos < tokens.length && !sym(tokens, pos, ',') && !sym(tokens, pos, ')')) {
        if (kw(tokens, pos, 'NOT') && (kw(tokens, pos + 1, 'NULL') || match(tokens, pos + 1, 'NULL'))) {
          nullable = false;
          pos += 2;
        } else if (kw(tokens, pos, 'PRIMARY') && kw(tokens, pos + 1, 'KEY')) {
          nullable = false;
          pos += 2;
        } else if (kw(tokens, pos, 'DEFAULT')) {
          pos++;
          const lit = parseLiteralValue(tokens, pos);
          defaultVal = lit.value;
          pos = lit.pos;
        } else if (kw(tokens, pos, 'UNIQUE') || kw(tokens, pos, 'CHECK') || kw(tokens, pos, 'REFERENCES') || kw(tokens, pos, 'NULL')) {
          pos++; // skip constraint keywords
          if (sym(tokens, pos, '(')) { // skip parenthesized args
            while (!sym(tokens, pos, ')') && pos < tokens.length) pos++;
            pos++;
          }
        } else {
          pos++;
        }
      }

      columns.push({ name: colName, type, nullable, default: defaultVal !== undefined ? defaultVal : null });
      if (sym(tokens, pos, ',')) pos++;
    }

    return new Event('create_table_execute', { table, columns, ifNotExists });
  }
}
