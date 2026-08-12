<?php

declare(strict_types=1);

namespace Changeflow;

/**
 * Reads the records changeflow writes for documents a destination refused permanently.
 *
 * These exist because something was already lost once, so a line that cannot be parsed
 * is an error rather than a skip: quietly ignoring one would lose it again.
 */
final class Dlq
{
    public function __construct(private readonly string $directory)
    {
    }

    /**
     * The dead letter files, newest last, for a stream or for every stream.
     *
     * @return array<int, string>
     */
    public function files(?string $stream = null): array
    {
        $pattern = $stream === null
            ? $this->directory . '/*.jsonl*'
            : $this->directory . '/' . $stream . '.jsonl*';

        $files = glob($pattern) ?: [];
        sort($files);

        return $files;
    }

    /**
     * Every record in a file.
     *
     * @return array<int, DeadLetter>
     */
    public function read(string $path): array
    {
        $handle = @fopen($path, 'rb');
        if ($handle === false) {
            throw new \RuntimeException(sprintf('cannot open dead letter file %s', $path));
        }

        try {
            $records = [];
            $line = 0;
            while (($raw = fgets($handle)) !== false) {
                ++$line;
                $raw = trim($raw);
                if ($raw === '') {
                    continue;
                }

                try {
                    /** @var array<string, mixed> $decoded */
                    $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
                } catch (\JsonException $e) {
                    throw new \RuntimeException(
                        sprintf('%s line %d is not a valid record: %s', $path, $line, $e->getMessage()),
                        0,
                        $e,
                    );
                }

                $records[] = DeadLetter::fromArray($decoded);
            }

            return $records;
        } finally {
            fclose($handle);
        }
    }

    /**
     * How many documents a stream has had refused, for a dashboard counter.
     *
     * Any value above zero is worth someone's attention: those documents are not in the
     * destination and will not arrive without intervention.
     */
    public function count(?string $stream = null): int
    {
        $total = 0;
        foreach ($this->files($stream) as $file) {
            $total += count($this->read($file));
        }

        return $total;
    }
}
