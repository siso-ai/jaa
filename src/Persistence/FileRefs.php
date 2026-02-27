<?php
namespace Ice\Persistence;

class FileRefs {
    private string $root;

    public function __construct(string $basePath) {
        $this->root = $basePath . '/refs';
        if (!is_dir($this->root)) {
            mkdir($this->root, 0777, true);
        }
    }

    public function set(string $name, string $hash): void {
        $path = $this->path($name);
        $dir = dirname($path);
        if (!is_dir($dir) && !mkdir($dir, 0777, true)) {
            throw new \RuntimeException("Cannot create ref directory: $dir");
        }

        $tmp = $path . '.tmp.' . getmypid();
        if (file_put_contents($tmp, $hash) === false) {
            throw new \RuntimeException("Cannot write ref: $name");
        }
        if (!rename($tmp, $path)) {
            @unlink($tmp);
            throw new \RuntimeException("Cannot rename ref: $name");
        }
    }

    public function get(string $name): ?string {
        $path = $this->path($name);
        if (!file_exists($path)) return null;
        return trim(file_get_contents($path));
    }

    public function delete(string $name): void {
        $path = $this->path($name);
        if (!file_exists($path)) return;
        unlink($path);
        $this->cleanEmptyDirs(dirname($path));
    }

    /** @return string[] sorted ref names matching prefix */
    public function list(string $prefix): array {
        $searchDir = $this->root . '/' . $prefix;

        if (!file_exists($searchDir)) {
            // prefix might point to a partial dir name — try parent
            $parent = dirname($searchDir);
            if (!is_dir($parent)) return [];
            $results = [];
            foreach ($this->walk($parent) as $f) {
                $name = $this->relativePath($f);
                if (str_starts_with($name, $prefix)) {
                    $results[] = $name;
                }
            }
            sort($results);
            return $results;
        }

        if (is_file($searchDir)) {
            return [$prefix];
        }

        $results = [];
        foreach ($this->walk($searchDir) as $f) {
            $results[] = $this->relativePath($f);
        }
        sort($results);
        return $results;
    }

    private function path(string $name): string {
        return $this->root . '/' . $name;
    }

    private function relativePath(string $fullPath): string {
        return substr($fullPath, strlen($this->root) + 1);
    }

    /** @return string[] all file paths under directory */
    private function walk(string $dir): array {
        $results = [];
        $dir = rtrim($dir, '/'); // prevent double-slash in constructed paths
        if (!is_dir($dir)) return $results;

        foreach (scandir($dir) as $entry) {
            if ($entry === '.' || $entry === '..') continue;
            if (str_contains($entry, '.tmp.')) continue;
            $full = $dir . '/' . $entry;
            if (is_dir($full)) {
                array_push($results, ...$this->walk($full));
            } else {
                $results[] = $full;
            }
        }
        return $results;
    }

    private function cleanEmptyDirs(string $dir): void {
        while ($dir !== $this->root && str_starts_with($dir, $this->root)) {
            $entries = @scandir($dir);
            if ($entries === false) break;
            $entries = array_diff($entries, ['.', '..']);
            if (count($entries) === 0) {
                @rmdir($dir);
                $dir = dirname($dir);
            } else {
                break;
            }
        }
    }
}
