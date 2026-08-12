<?php

declare(strict_types=1);

namespace Changeflow;

/**
 * Thrown when the checkpoint table was written by a newer changeflow than this package
 * understands.
 *
 * Refusing to interpret the row is the point. Fields are added over time and never
 * repurposed, but a reader that guessed at an unknown layout would report progress it
 * has no basis for.
 */
final class SchemaVersionMismatch extends \RuntimeException
{
    public function __construct(
        public readonly string $stream,
        public readonly int $found,
        public readonly int $supported,
    ) {
        parent::__construct(sprintf(
            'stream %s was written with checkpoint schema version %d, and this package understands %d; upgrade changeflow/changeflow',
            $stream,
            $found,
            $supported,
        ));
    }
}
