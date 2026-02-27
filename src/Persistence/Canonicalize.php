<?php
namespace Ice\Persistence;

/**
 * Deterministic JSON serialization.
 * Same content always produces the same string.
 * Object keys sorted recursively. Arrays preserve order.
 */
function canonicalize(mixed $value): string {
    if ($value === null) return 'null';
    if (is_bool($value)) return $value ? 'true' : 'false';
    if (is_int($value)) return (string)$value;
    if (is_float($value)) return json_encode($value);
    if (is_string($value)) return json_encode($value);

    if (is_array($value)) {
        // Sequential array vs associative
        if (array_is_list($value)) {
            $items = array_map(fn($v) => canonicalize($v), $value);
            return '[' . implode(',', $items) . ']';
        } else {
            $keys = array_keys($value);
            sort($keys);
            $pairs = [];
            foreach ($keys as $k) {
                if ($value[$k] === null && !array_key_exists($k, $value)) continue;
                $pairs[] = json_encode((string)$k) . ':' . canonicalize($value[$k]);
            }
            return '{' . implode(',', $pairs) . '}';
        }
    }

    throw new \RuntimeException("Cannot canonicalize type: " . gettype($value));
}
