/**
 * ReadSet — declares what state a gate needs.
 *
 * A list of specific ref names and prefix patterns.
 * The resolution layer (Phase 3) resolves these against
 * persistence before calling the gate's transform.
 *
 * Builder pattern — chain .ref() and .pattern() calls.
 * The gate constructs this in reads(event). It never
 * touches persistence itself.
 */
export class ReadSet {
  constructor() {
    this.refs = [];      // specific ref names to resolve
    this.patterns = [];  // prefix patterns to list and resolve
  }

  /**
   * Add a specific ref name to resolve.
   * e.g. "db/tables/users/schema"
   */
  ref(name) {
    this.refs.push(name);
    return this;
  }

  /**
   * Add a prefix pattern. The resolution layer will list
   * all refs matching this prefix and resolve each.
   * e.g. "db/tables/users/rows/"
   */
  pattern(prefix) {
    this.patterns.push(prefix);
    return this;
  }
}
