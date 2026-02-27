import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { kw, sym } from './parser-utils.js';

export class IndexCreateParseGate extends PureGate {
  constructor() { super('index_create_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 1; // skip CREATE

    let unique = false;
    if (kw(tokens, pos, 'UNIQUE')) { unique = true; pos++; }
    pos++; // skip INDEX

    const index = tokens[pos].value;
    pos++;

    // ON table
    pos++; // skip ON
    const table = tokens[pos].value;
    pos++;

    // (column)
    pos++; // skip (
    const column = tokens[pos].value;
    pos++;

    return new Event('index_create_execute', { table, index, column, unique });
  }
}

export class IndexDropParseGate extends PureGate {
  constructor() { super('index_drop_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 2; // skip DROP INDEX
    const index = tokens[pos].value;
    pos++;

    // ON table
    pos++; // skip ON
    const table = tokens[pos].value;

    return new Event('index_drop_execute', { table, index });
  }
}
