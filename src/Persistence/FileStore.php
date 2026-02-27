<?php
namespace Ice\Persistence;

class FileStore {
    private string $root;

    public function __construct(string $basePath) {
        $this->root = $basePath . '/store';
        if (!is_dir($this->root)) {
            mkdir($this->root, 0777, true);
        }
    }

    public function put(mixed $content): string {
        $canonical = canonicalize($content);
        $hash = hash('sha256', $canonical);
        $path = $this->path($hash);

        if (file_exists($path)) return $hash; // dedup

        $dir = dirname($path);
        if (!is_dir($dir) && !mkdir($dir, 0777, true)) {
            throw new \RuntimeException("Cannot create store directory: $dir");
        }

        // Atomic write: temp + rename
        $tmp = $path . '.tmp.' . getmypid();
        if (file_put_contents($tmp, $canonical) === false) {
            throw new \RuntimeException("Cannot write object: $hash");
        }
        if (!rename($tmp, $path)) {
            @unlink($tmp);
            throw new \RuntimeException("Cannot rename object: $hash");
        }

        return $hash;
    }

    public function get(string $hash): mixed {
        $path = $this->path($hash);
        if (!file_exists($path)) {
            throw new \RuntimeException("Object not found: $hash");
        }
        return json_decode(file_get_contents($path), true);
    }

    public function has(string $hash): bool {
        return file_exists($this->path($hash));
    }

    private function path(string $hash): string {
        return $this->root . '/' . substr($hash, 0, 2) . '/' . substr($hash, 2);
    }
}
