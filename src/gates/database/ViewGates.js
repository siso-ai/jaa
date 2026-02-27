/**
 * View gates — create, drop, and expand views.
 */
import { StateGate } from '../../protocol/StateGate.js';
import { ReadSet } from '../../protocol/ReadSet.js';
import { MutationBatch } from '../../protocol/MutationBatch.js';
import { Event } from '../../core/Event.js';

export class ViewCreateExecuteGate extends StateGate {
  constructor() { super('view_create_execute'); }

  reads(event) {
    return new ReadSet().ref(`db/views/${event.data.name}`);
  }

  transform(event, state) {
    const { name, query, columns } = event.data;
    const existing = state.refs[`db/views/${name}`];

    if (existing !== null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `View '${name}' already exists`, source: 'view_create_execute' }));
    }

    return new MutationBatch()
      .put('view', { name, query, columns })
      .refSet(`db/views/${name}`, 0)
      .emit(new Event('view_created', { name }));
  }
}

export class ViewDropExecuteGate extends StateGate {
  constructor() { super('view_drop_execute'); }

  reads(event) {
    return new ReadSet().ref(`db/views/${event.data.name}`);
  }

  transform(event, state) {
    const { name } = event.data;
    if (state.refs[`db/views/${name}`] === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `View '${name}' does not exist`, source: 'view_drop_execute' }));
    }

    return new MutationBatch()
      .refDelete(`db/views/${name}`)
      .emit(new Event('view_dropped', { name }));
  }
}

export class ViewExpansionGate extends StateGate {
  constructor() { super('view_expand'); }

  reads(event) {
    return new ReadSet().ref(`db/views/${event.data.view}`);
  }

  transform(event, state) {
    const viewDef = state.refs[`db/views/${event.data.view}`];

    if (viewDef === null) {
      return new MutationBatch()
        .emit(new Event('error', { message: `View '${event.data.view}' does not exist`, source: 'view_expand' }));
    }

    // Re-emit the stored query as a query_plan event
    return new MutationBatch()
      .emit(new Event('query_plan', { pipeline: viewDef.query.pipeline || [] }));
  }
}
