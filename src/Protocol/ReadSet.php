<?php
namespace Ice\Protocol;

class ReadSet {
    /** @var string[] */
    private array $refs = [];
    /** @var string[] */
    private array $patterns = [];

    public function ref(string $name): self {
        $this->refs[] = $name;
        return $this;
    }

    public function pattern(string $prefix): self {
        $this->patterns[] = $prefix;
        return $this;
    }

    public function getRefs(): array { return $this->refs; }
    public function getPatterns(): array { return $this->patterns; }
    public function isEmpty(): bool { return empty($this->refs) && empty($this->patterns); }
}
