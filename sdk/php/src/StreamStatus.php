<?php

declare(strict_types=1);

namespace Changeflow;

/**
 * One stream's replication state, as recorded by changeflow.
 *
 * Read-only by construction. Nothing here can affect replication, which is deliberate:
 * an admin panel that can edit a position can also corrupt one.
 */
final readonly class StreamStatus
{
    public function __construct(
        public string $stream,
        /** The transactions applied and acknowledged so far. */
        public string $position,
        public bool $snapshotDone,
        public int $snapshotRowsDone,
        /** An estimate from table statistics, so a percentage derived from it is approximate. */
        public int $snapshotRowsEstimated,
        /** When the source recorded the most recent applied change, or null before any. */
        public ?\DateTimeImmutable $lastEventAt,
        public ?string $lastError,
        public \DateTimeImmutable $updatedAt,
    ) {
    }

    /**
     * Seconds between the source recording the most recent applied change and now.
     *
     * Null rather than zero when nothing has been applied yet: a stream that has never
     * seen a change is not the same as one that is caught up, and showing them alike on
     * a dashboard hides the difference that matters.
     */
    public function lagSeconds(?\DateTimeImmutable $now = null): ?float
    {
        if ($this->lastEventAt === null) {
            return null;
        }

        $now ??= new \DateTimeImmutable('now', new \DateTimeZone('UTC'));
        $lag = (float) $now->format('U.u') - (float) $this->lastEventAt->format('U.u');

        // A source clock reading ahead of ours is not negative lag.
        return max(0.0, $lag);
    }

    /**
     * Whether this stream is keeping up.
     *
     * A quiet table produces no changes, so a stream with nothing applied yet is not
     * behind. Only a change that was applied too long ago means one.
     */
    public function isHealthy(float $maxLagSeconds = 300.0, ?\DateTimeImmutable $now = null): bool
    {
        if ($this->lastError !== null && $this->lastError !== '') {
            return false;
        }

        $lag = $this->lagSeconds($now);

        return $lag === null || $lag <= $maxLagSeconds;
    }

    /**
     * How far an initial table scan has progressed, from 0 to 1, or null when there is
     * nothing meaningful to report.
     */
    public function snapshotProgress(): ?float
    {
        if ($this->snapshotDone) {
            return 1.0;
        }
        if ($this->snapshotRowsEstimated <= 0) {
            // The estimate comes from table statistics and is sometimes zero. Inventing a
            // percentage from it would be worse than admitting there is none.
            return null;
        }

        return min(1.0, $this->snapshotRowsDone / $this->snapshotRowsEstimated);
    }

    /** A short description for a status line. */
    public function describe(?\DateTimeImmutable $now = null): string
    {
        if (!$this->snapshotDone) {
            $progress = $this->snapshotProgress();

            return $progress === null
                ? sprintf('scanning, %d rows read', $this->snapshotRowsDone)
                : sprintf('scanning, %d%%', (int) round($progress * 100));
        }

        $lag = $this->lagSeconds($now);

        return $lag === null
            ? 'streaming, no changes yet'
            : sprintf('streaming, %.1fs behind', $lag);
    }
}
