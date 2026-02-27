<?php
namespace Ice\Persistence;

class MemoryStore {
    /** @var array<string, string> hash → canonical JSON */
    private array $objects = [];

    public function put(mixed $content): string {
        $canonical = canonicalize($content);
        $hash = hash('sha256', $canonical);
        if (!isset($this->objects[$hash])) {
            $this->objects[$hash] = $canonical;
        }
        return $hash;
    }

    public function get(string $hash): mixed {
        if (!isset($this->objects[$hash])) {
            throw new \RuntimeException("Object not found: $hash");
        }
        return json_decode($this->objects[$hash], true);
    }

    public function has(string $hash): bool {
        return isset($this->objects[$hash]);
    }

    // For Runner snapshot/restore
    public function getObjects(): array { return $this->objects; }
    public function setObjects(array $objects): void { $this->objects = $objects; }
}
