<?php

declare(strict_types=1);

namespace Changeflow;

/**
 * Builds changeflow's stream configuration from an application's own metadata.
 *
 * This is the one place the two sides genuinely share knowledge. An application that
 * already knows the shape of its search documents can emit that here, rather than
 * someone restating it in YAML where the two drift apart silently. The generated file is
 * committed and checked by `changeflow validate` in CI, so a model change that breaks the
 * export fails the build instead of production.
 *
 * It writes configuration and nothing else: starting, stopping, and rescanning are
 * operator actions with real blast radius, and they stay in the command line tool.
 */
final class ConfigBuilder
{
    /** @var array<string, array<string, mixed>> */
    private array $streams = [];

    /** @var array<string, mixed> */
    private array $source = [];

    /** @var array<string, mixed> */
    private array $checkpoint = [];

    /** @var array<string, mixed> */
    private array $runtime = [];

    /**
     * @param string $dsn      how changeflow reaches the source, usually an environment reference
     * @param int    $serverId must be unique among the source's replicas, including the source itself
     */
    public function source(string $dsn, int $serverId, ?string $timeZone = null): self
    {
        $this->source = array_filter([
            'dsn' => $dsn,
            'server_id' => $serverId,
            'time_zone' => $timeZone,
        ], static fn ($v): bool => $v !== null);

        return $this;
    }

    public function checkpoint(string $dsn, ?string $table = null): self
    {
        $this->checkpoint = array_filter([
            'dsn' => $dsn,
            'table' => $table,
        ], static fn ($v): bool => $v !== null);

        return $this;
    }

    /** @param array<string, mixed> $settings */
    public function runtime(array $settings): self
    {
        $this->runtime = $settings;

        return $this;
    }

    /**
     * Add a stream writing to Elasticsearch.
     *
     * @param string             $name    stream name, which also names its checkpoint row
     * @param string             $table   source table as database.table
     * @param list<string>       $key     columns identifying a row, defaulting to the primary key
     * @param list<string>       $include columns to write, or all of them when empty
     * @param array<string,string> $rename source column to destination field
     */
    public function elasticsearchStream(
        string $name,
        string $table,
        string $index,
        array $addresses,
        array $key = [],
        array $include = [],
        array $rename = [],
        ?string $alias = null,
    ): self {
        $this->addStream($name, $table, $key, $include, $rename, array_filter([
            'type' => 'elasticsearch',
            'addresses' => $addresses,
            'index' => $index,
            'alias' => $alias,
        ], static fn ($v): bool => $v !== null));

        return $this;
    }

    /** Add a stream writing to ClickHouse. */
    public function clickHouseStream(
        string $name,
        string $table,
        string $dsn,
        string $destinationTable,
        array $key = [],
        array $include = [],
        array $rename = [],
    ): self {
        $this->addStream($name, $table, $key, $include, $rename, [
            'type' => 'clickhouse',
            'dsn' => $dsn,
            'table' => $destinationTable,
        ]);

        return $this;
    }

    /** @param array<string, mixed> $sink */
    private function addStream(
        string $name,
        string $table,
        array $key,
        array $include,
        array $rename,
        array $sink,
    ): void {
        if (!preg_match('/^[A-Za-z0-9_]+$/', $name)) {
            throw new \InvalidArgumentException(
                sprintf('stream name %s may contain only letters, digits, and underscore', $name)
            );
        }
        // The checkpoint table's column bounds this. A longer name would produce a stream
        // that runs but can never record a position.
        if (strlen($name) > 48) {
            throw new \InvalidArgumentException(
                sprintf('stream name %s is %d characters; changeflow allows at most 48', $name, strlen($name))
            );
        }
        if (!str_contains($table, '.')) {
            throw new \InvalidArgumentException(
                sprintf('table %s must be written as database.table', $table)
            );
        }
        if (isset($this->streams[$name])) {
            throw new \InvalidArgumentException(sprintf('stream %s is already defined', $name));
        }

        // A key column that is not written cannot identify a row in the destination.
        if ($include !== []) {
            foreach ($key as $column) {
                if (!in_array($column, $include, true)) {
                    throw new \InvalidArgumentException(sprintf(
                        'stream %s includes %d columns but not the key column %s',
                        $name,
                        count($include),
                        $column,
                    ));
                }
            }
        }

        $mapping = array_filter([
            'key' => $key,
            'include' => $include,
            'rename' => $rename,
        ], static fn ($v): bool => $v !== []);

        $this->streams[$name] = array_filter([
            'table' => $table,
            'sink' => $sink,
            'mapping' => $mapping,
        ], static fn ($v): bool => $v !== []);
    }

    /**
     * Render the configuration as YAML.
     *
     * Written by hand rather than with a YAML library, so the package needs no extension
     * beyond json and pdo, and so the output stays stable enough to diff between runs.
     */
    public function toYaml(): string
    {
        if ($this->source === []) {
            throw new \LogicException('a source is required');
        }
        if ($this->checkpoint === []) {
            throw new \LogicException('a checkpoint store is required');
        }
        if ($this->streams === []) {
            throw new \LogicException('at least one stream is required');
        }

        $lines = ['# Generated by Changeflow\\ConfigBuilder. Commit this file and let CI check it', '# with `changeflow validate`.', ''];

        $lines[] = 'source:';
        $lines = array_merge($lines, self::render($this->source, 1));

        $lines[] = 'checkpoint:';
        $lines = array_merge($lines, self::render($this->checkpoint, 1));

        if ($this->runtime !== []) {
            $lines[] = 'runtime:';
            $lines = array_merge($lines, self::render($this->runtime, 1));
        }

        $lines[] = 'streams:';
        // Sorted, so a regenerated file produces a reviewable diff rather than noise from
        // an incidental ordering change.
        $streams = $this->streams;
        ksort($streams);
        foreach ($streams as $name => $stream) {
            $lines[] = '  ' . $name . ':';
            $lines = array_merge($lines, self::render($stream, 2));
        }

        return implode("\n", $lines) . "\n";
    }

    /** Write the configuration to a file, creating the directory when needed. */
    public function writeTo(string $path): void
    {
        $directory = dirname($path);
        if (!is_dir($directory) && !mkdir($directory, 0o755, true) && !is_dir($directory)) {
            throw new \RuntimeException(sprintf('cannot create %s', $directory));
        }
        if (file_put_contents($path, $this->toYaml()) === false) {
            throw new \RuntimeException(sprintf('cannot write %s', $path));
        }
    }

    /**
     * @param array<string, mixed> $values
     * @return array<int, string>
     */
    private static function render(array $values, int $depth): array
    {
        $indent = str_repeat('  ', $depth);
        $lines = [];

        foreach ($values as $field => $value) {
            if (is_array($value) && $value !== [] && array_is_list($value)) {
                $rendered = array_map([self::class, 'scalar'], $value);
                $lines[] = $indent . $field . ': [' . implode(', ', $rendered) . ']';
                continue;
            }
            if (is_array($value)) {
                $lines[] = $indent . $field . ':';
                $lines = array_merge($lines, self::render($value, $depth + 1));
                continue;
            }
            $lines[] = $indent . $field . ': ' . self::scalar($value);
        }

        return $lines;
    }

    private static function scalar(mixed $value): string
    {
        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }
        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        // Quoted, so a value containing a colon, a leading digit, or an environment
        // reference cannot change the meaning of the line.
        return '"' . str_replace(['\\', '"'], ['\\\\', '\\"'], (string) $value) . '"';
    }
}
