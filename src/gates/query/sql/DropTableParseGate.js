import { PureGate } from '../../../protocol/PureGate.js';
import { Event } from '../../../core/Event.js';
import { kw } from './parser-utils.js';

export class DropTableParseGate extends PureGate {
  constructor() { super('drop_table_parse'); }

  transform(event) {
    const tokens = event.data.tokens;
    let pos = 2; // skip DROP TABLE

    let ifExists = false;
    if (kw(tokens, pos, 'IF')) {
      ifExists = true;
      pos += 2; // skip IF EXISTS
    }

    const table = tokens[pos].value;
    return new Event('drop_table_execute', { table, ifExists });
  }
}
