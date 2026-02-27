<?php
namespace Ice\Persistence;

class MemoryRefs {
    /** @var array<string, string> name → hash */
    private array $refs = [];

    public function set(string $name, string $hash): void {
        $this->refs[$name] = $hash;
    }

    public function get(string $name): ?string {
        return $this->refs[$name] ?? null;
    }

    public function delete(string $name): void {
        unset($this->refs[$name]);
    }

    /** @return string[] sorted ref names matching prefix */
    public function list(string $prefix): array {
        $matches = [];
        foreach ($this->refs as $name => $_) {
            if (str_starts_with($name, $prefix)) {
                $matches[] = $name;
            }
        }
        sort($matches);
        return $matches;
    }

    // For Runner snapshot/restore
    public function getRefs(): array { return $this->refs; }
    public function setRefs(array $refs): void { $this->refs = $refs; }
}
