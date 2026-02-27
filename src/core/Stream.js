/**
 * Stream — the processing loop.
 *
 * Gates register by signature. Events arrive via emit().
 * The stream looks up the event's type in the gate table.
 * If a gate claims it, transform runs immediately (depth-first).
 * If nothing claims it, the event lands in pending.
 *
 * Pending events are the residue — what's left when
 * processing settles. sampleHere() reads them.
 *
 * If a StreamLog is provided, emit() records every decision.
 * The log is shared — sub-streams write to the same one,
 * giving full visibility across the tree.
 *
 * This is →E→E→.
 */
export class Stream {
  constructor({ log = null, parentStreamId = null } = {}) {
    this.gates = new Map();
    this.pending = [];
    this.eventCount = 0;
    this.log = log;
    this.streamId = log ? log.nextStreamId() : null;
    this.parentStreamId = parentStreamId;
  }

  /**
   * Register a gate. Signature must be unique.
   * Collision is a hard error, not a silent precedence bug.
   */
  register(gate) {
    if (this.gates.has(gate.signature)) {
      throw new Error(`Signature collision: '${gate.signature}'`);
    }
    this.gates.set(gate.signature, gate);
  }

  /**
   * Emit an event into the stream.
   * Processed immediately, depth-first.
   * If no gate claims it, it lands in pending.
   */
  emit(event) {
    this.eventCount++;
    const gate = this.gates.get(event.type);

    if (this.log) {
      this.log.record({
        streamId: this.streamId,
        parentStreamId: this.parentStreamId,
        eventType: event.type,
        gateClaimed: gate ? gate.signature : null,
        eventData: event.data,
      });
    }

    if (gate) {
      gate.transform(event, this);
    } else {
      this.pending.push(event);
    }
  }

  /**
   * Not getResult. The flow is ongoing.
   * We choose to look now. What we read
   * is a cross-section of →E→E→.
   */
  sampleHere() {
    return {
      pending: [...this.pending],
      eventCount: this.eventCount,
      gateCount: this.gates.size,
    };
  }
}
