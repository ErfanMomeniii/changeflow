<?php

declare(strict_types=1);

namespace Changeflow;

/**
 * Reads changeflow's replication state.
 *
 * It queries the checkpoint table directly rather than asking a running process, so it
 * works when a stream is down, which is when its state is most worth knowing. Only
 * SELECT is used, and the database user should be granted no more than that.
 */
final class Status
{
    /**
     * The checkpoint layout this package understands.
     *
     * A row written with a higher version is refused rather than misread.
     */
    public const SUPPORTED_SCHEMA_VERSION = 1;

    public function __construct(
        private readonly \PDO $pdo,
        private readonly string $table = 'changeflow_checkpoints',
    ) {
        if (!preg_match('/^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$/', $this->table)) {
            // The table name reaches SQL, where an identifier cannot be bound as a
            // parameter, so it is validated rather than escaped.
            throw new \InvalidArgumentException(sprintf('table name %s is not a plain identifier', $this->table));
        }
    }

    /**
     * Every stream changeflow knows about, ordered by name so a page does not reshuffle
     * between refreshes.
     *
     * @return array<int, StreamStatus>
     */
    public function streams(): array
    {
        $rows = $this->pdo
            ->query(sprintf('SELECT %s FROM %s ORDER BY stream', self::columns(), $this->table))
            ->fetchAll(\PDO::FETCH_ASSOC);

        return array_map([$this, 'toStatus'], $rows);
    }

    /** One stream, or null when changeflow has never run it. */
    public function stream(string $name): ?StreamStatus
    {
        $statement = $this->pdo->prepare(
            sprintf('SELECT %s FROM %s WHERE stream = ?', self::columns(), $this->table)
        );
        $statement->execute([$name]);
        $row = $statement->fetch(\PDO::FETCH_ASSOC);

        return $row === false ? null : $this->toStatus($row);
    }

    /**
     * Whether every stream is keeping up, for a health endpoint.
     *
     * A configuration with no streams yet reports healthy: there is nothing failing.
     */
    public function isHealthy(float $maxLagSeconds = 300.0, ?\DateTimeImmutable $now = null): bool
    {
        foreach ($this->streams() as $stream) {
            if (!$stream->isHealthy($maxLagSeconds, $now)) {
                return false;
            }
        }

        return true;
    }

    /**
     * The streams that are not keeping up, so a dashboard can show what to look at
     * rather than only that something is wrong.
     *
     * @return array<int, StreamStatus>
     */
    public function unhealthy(float $maxLagSeconds = 300.0, ?\DateTimeImmutable $now = null): array
    {
        return array_values(array_filter(
            $this->streams(),
            static fn (StreamStatus $s): bool => !$s->isHealthy($maxLagSeconds, $now),
        ));
    }

    private static function columns(): string
    {
        // Named explicitly, so a column added by a newer changeflow cannot change what
        // this query returns.
        return 'stream, gtid_set, snapshot_done, snapshot_rows_done, snapshot_rows_total, '
            . 'last_event_ts_ms, last_error, schema_version, updated_at';
    }

    /** @param array<string, mixed> $row */
    private function toStatus(array $row): StreamStatus
    {
        $version = (int) $row['schema_version'];
        if ($version > self::SUPPORTED_SCHEMA_VERSION) {
            throw new SchemaVersionMismatch((string) $row['stream'], $version, self::SUPPORTED_SCHEMA_VERSION);
        }

        $lastError = $row['last_error'] === null ? null : trim((string) $row['last_error']);

        return new StreamStatus(
            stream: (string) $row['stream'],
            position: (string) $row['gtid_set'],
            snapshotDone: (bool) $row['snapshot_done'],
            snapshotRowsDone: (int) $row['snapshot_rows_done'],
            snapshotRowsEstimated: (int) $row['snapshot_rows_total'],
            lastEventAt: self::instantFromMillis((int) $row['last_event_ts_ms']),
            lastError: $lastError === '' ? null : $lastError,
            updatedAt: self::instantFromString((string) $row['updated_at']),
        );
    }

    private static function instantFromMillis(int $millis): ?\DateTimeImmutable
    {
        if ($millis <= 0) {
            // Nothing has been applied yet, which is distinct from being caught up.
            return null;
        }

        return \DateTimeImmutable::createFromFormat(
            'U.u',
            sprintf('%d.%03d', intdiv($millis, 1000), $millis % 1000),
            new \DateTimeZone('UTC'),
        )->setTimezone(new \DateTimeZone('UTC'));
    }

    private static function instantFromString(string $value): \DateTimeImmutable
    {
        // The column is a TIMESTAMP, which MySQL returns without a zone. changeflow
        // stores UTC, so it is read as UTC rather than as the reader's local time.
        return new \DateTimeImmutable($value, new \DateTimeZone('UTC'));
    }
}
