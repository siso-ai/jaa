/**
 * canonicalize — deterministic JSON serialization.
 *
 * Same content always produces the same string.
 * Object keys are sorted recursively. Arrays preserve order.
 * Used by Store.put() to ensure identical content
 * produces identical hashes.
 */
export function canonicalize(value) {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'boolean') return value.toString();
  if (typeof value === 'number') return JSON.stringify(value);
  if (typeof value === 'string') return JSON.stringify(value);

  if (Array.isArray(value)) {
    const items = value.map(v => canonicalize(v));
    return '[' + items.join(',') + ']';
  }

  if (typeof value === 'object') {
    const keys = Object.keys(value).sort();
    const pairs = keys
      .filter(k => value[k] !== undefined)
      .map(k => JSON.stringify(k) + ':' + canonicalize(value[k]));
    return '{' + pairs.join(',') + '}';
  }

  throw new Error(`Cannot canonicalize type: ${typeof value}`);
}
