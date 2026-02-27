<?php
/**
 * ICE Database — Minimal test harness.
 * Usage: php test/test.php
 */

$passed = 0;
$failed = 0;
$errors = [];
$currentSection = '';

function section(string $name): void {
    global $currentSection;
    $currentSection = $name;
    echo "\n  $name\n";
}

function test(string $name, callable $fn): void {
    global $passed, $failed, $errors, $currentSection;
    try {
        $fn();
        $passed++;
        echo "    ✓ $name\n";
    } catch (\Throwable $e) {
        $failed++;
        $errors[] = "$currentSection: $name — {$e->getMessage()} at {$e->getFile()}:{$e->getLine()}";
        echo "    ✗ $name\n      {$e->getMessage()}\n";
    }
}

function assertEqual(mixed $actual, mixed $expected, string $msg = ''): void {
    if ($actual !== $expected) {
        $a = json_encode($actual);
        $e = json_encode($expected);
        throw new \RuntimeException($msg ?: "Expected $e, got $a");
    }
}

function assertTrue(bool $value, string $msg = ''): void {
    if (!$value) throw new \RuntimeException($msg ?: "Expected true, got false");
}

function assertFalse(bool $value, string $msg = ''): void {
    if ($value) throw new \RuntimeException($msg ?: "Expected false, got true");
}

function assertNull(mixed $value, string $msg = ''): void {
    if ($value !== null) throw new \RuntimeException($msg ?: "Expected null, got " . json_encode($value));
}

function assertThrows(callable $fn, string $msg = ''): void {
    try {
        $fn();
        throw new \RuntimeException($msg ?: "Expected exception, none thrown");
    } catch (\RuntimeException $e) {
        if ($e->getMessage() === ($msg ?: "Expected exception, none thrown")) throw $e;
        // Good — exception was thrown
    }
}

function assertCount(int $expected, array $arr, string $msg = ''): void {
    $actual = count($arr);
    if ($actual !== $expected) {
        throw new \RuntimeException($msg ?: "Expected count $expected, got $actual");
    }
}

function report(): void {
    global $passed, $failed, $errors;
    echo "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n";
    echo "  $passed passed, $failed failed\n";
    if ($failed > 0) {
        echo "\n  Failures:\n";
        foreach ($errors as $e) echo "    • $e\n";
        echo "\n";
        exit(1);
    }
    echo "\n";
}
