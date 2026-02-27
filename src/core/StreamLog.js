/**
 * StreamLog — the audit trail.
 *
 * A shared object that streams write to. One log sees
 * every event across a stream and its sub-streams.
 * Not a gate. Not a primitive. Self-observation by
 * the infrastructure that already makes the decisions.
 *
 * Levels (each includes everything below):
 *   OFF    — nothing
 *   EVENTS — sequence, timestamp, type, claimed/pending
 *   DEEP   — sub-streams participate with parent/child IDs
 *   DATA   — includes event payloads
 *
 * Level is mutable at runtime. Change it and the next
 * emit() respects it immediately.
 */

const LEVELS = { OFF: 0, EVENTS: 1, DEEP: 2, DATA: 3 };

let streamIdCounter = 0;

export class StreamLog {
  constructor(level = 'EVENTS') {
    this.level = level;
    this.entries = [];
    this.seq = 0;
  }

  /**
   * Called by Stream.emit(). The stream passes what it
   * already knows — no extra work unless logging is on.
   */
  record({ streamId, parentStreamId, eventType, gateClaimed, eventData }) {
    const lvl = LEVELS[this.level] || 0;
    if (lvl === 0) return;

    const entry = {
      seq: this.seq++,
      time: Date.now(),
      type: eventType,
      claimed: gateClaimed,
    };

    // DEEP — include stream lineage
    if (lvl >= 2) {
      entry.streamId = streamId;
      if (parentStreamId != null) {
        entry.parentStreamId = parentStreamId;
      }
    }

    // DATA — include payload
    if (lvl >= 3) {
      entry.data = eventData;
    }

    this.entries.push(entry);
  }

  /**
   * Generate a unique stream ID. Called by Stream
   * on construction when a log is present.
   */
  nextStreamId() {
    return ++streamIdCounter;
  }

  /**
   * Read the full log. Same philosophy as sampleHere —
   * observation, not consumption.
   */
  sample() {
    return {
      level: this.level,
      count: this.entries.length,
      entries: [...this.entries],
    };
  }

  /**
   * Clear the log. The one concession to mutability —
   * for long-running systems that need to shed history.
   */
  clear() {
    this.entries = [];
    this.seq = 0;
  }
}
