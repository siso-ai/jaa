/**
 * Constraint gates — create and drop constraints.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';

export class ConstraintCreateExecuteGate extends StateGate {
  constructor() { super('constraint_create_execute'); }

  reads(event) {
    const { table, name } = event.data;
    return new ReadSet()
      .ref(`db/tables/${table}/schema`)
      .ref(`db/constraints/${table}/${name}`);
  }

  transform(event, state) {
    const { table, name, type, params } = event.data;
    const schema = state.refs[`db/tables/${table}/schema`];
    const existing = state.refs[`db/constraints/${table}/${name}`];

    if (schema === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Table '${table}' does not exist`, source: 'constraint_create_execute' }));
    }

    if (existing !== null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Constraint '${name}' already exists`, source: 'constraint_create_execute' }));
    }

    return new MutationBatch()
      .put('constraint', { name, table, type, params })
      .refSet(`db/constraints/${table}/${name}`, 0)
      .emit(new Event('constraint_created', { table, name }));
  }
}

export class ConstraintDropExecuteGate extends StateGate {
  constructor() { super('constraint_drop_execute'); }

  reads(event) {
    return new ReadSet()
      .ref(`db/constraints/${event.data.table}/${event.data.name}`);
  }

  transform(event, state) {
    const { table, name } = event.data;
    if (state.refs[`db/constraints/${table}/${name}`] === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `Constraint '${name}' does not exist`, source: 'constraint_drop_execute' }));
    }

    return new MutationBatch()
      .refDelete(`db/constraints/${table}/${name}`)
      .emit(new Event('constraint_dropped', { table, name }));
  }
}
