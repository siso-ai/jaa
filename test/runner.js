/**
 * Minimal test runner. No dependencies.
 * Reports pass/fail, stops on first failure in a test,
 * continues to next test.
 */

let passed = 0;
let failed = 0;
let currentTest = '';
const failures = [];

export function test(name, fn) {
  currentTest = name;
  try {
    fn();
    passed++;
  } catch (e) {
    failed++;
    failures.push({ name, message: e.message });
    console.log(`  ✗  ${name}`);
    console.log(`     ${e.message}`);
  }
}

export function assert(condition, message = 'assertion failed') {
  if (!condition) throw new Error(message);
}

export function assertEqual(actual, expected, label = '') {
  if (actual !== expected) {
    const prefix = label ? `${label}: ` : '';
    throw new Error(`${prefix}expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
}

export function assertClose(actual, expected, epsilon = 1e-10, label = '') {
  if (Math.abs(actual - expected) > epsilon) {
    const prefix = label ? `${label}: ` : '';
    throw new Error(`${prefix}expected ≈${expected}, got ${actual}`);
  }
}

export function assertThrows(fn, substring = '', label = '') {
  try {
    fn();
    const prefix = label ? `${label}: ` : '';
    throw new Error(`${prefix}expected error, none thrown`);
  } catch (e) {
    if (substring && !e.message.includes(substring)) {
      const prefix = label ? `${label}: ` : '';
      throw new Error(`${prefix}expected error containing '${substring}', got '${e.message}'`);
    }
  }
}

export function section(name) {
  // Visual separator in output (no-op for test logic)
}

export function report(suite) {
  const total = passed + failed;
  console.log();
  if (failed === 0) {
    console.log(`${suite}: ${passed}/${total} passed`);
  } else {
    console.log(`${suite}: ${passed}/${total} passed, ${failed} FAILED`);
    for (const f of failures) {
      console.log(`  FAIL: ${f.name} — ${f.message}`);
    }
  }
  return failed;
}

export function reset() {
  passed = 0;
  failed = 0;
  failures.length = 0;
}
