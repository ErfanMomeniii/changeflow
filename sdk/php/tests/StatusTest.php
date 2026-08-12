<?php

declare(strict_types=1);

namespace Changeflow\Tests;

use Changeflow\SchemaVersionMismatch;
use Changeflow\Status;
use PHPUnit\Framework\TestCase;

/**
 * Status is exercised against a real database engine, using SQLite because the queries
 * are plain SQL and what is under test is the reading logic rather than the driver.
 */
final class StatusTest extends TestCase
{
    private \PDO $pdo;

    protected function setUp(): void
    {
        $this->pdo = new \PDO('sqlite::memory:', null, null, [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
        ]);
        $this->pdo->exec(
            'CREATE TABLE changeflow_checkpoints (
                stream TEXT PRIMARY KEY,
                gtid_set TEXT NOT NULL,
                snapshot_done INTEGER NOT NULL DEFAULT 0,
                snapshot_rows_done INTEGER NOT NULL DEFAULT 0,
                snapshot_rows_total INTEGER NOT NULL DEFAULT 0,
                last_event_ts_ms INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                schema_version INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT NOT NULL
            )'
        );
    }

    /** @param array<string, mixed> $values */
    private function insert(array $values = []): void
    {
        $row = array_merge([
            'stream' => 'orders_to_es',
            'gtid_set' => 'uuid:1-100',
            'snapshot_done' => 1,
            'snapshot_rows_done' => 0,
            'snapshot_rows_total' => 0,
            'last_event_ts_ms' => 0,
            'last_error' => null,
            'schema_version' => 1,
            'updated_at' => '2026-08-11 12:00:00',
        ], $values);

        $statement = $this->pdo->prepare(
            'INSERT INTO changeflow_checkpoints
             (stream, gtid_set, snapshot_done, snapshot_rows_done, snapshot_rows_total,
              last_event_ts_ms, last_error, schema_version, updated_at)
             VALUES (:stream, :gtid_set, :snapshot_done, :snapshot_rows_done, :snapshot_rows_total,
                     :last_event_ts_ms, :last_error, :schema_version, :updated_at)'
        );
        $statement->execute($row);
    }

    private function reader(): Status
    {
        return new Status($this->pdo);
    }

    private static function at(string $when): \DateTimeImmutable
    {
        return new \DateTimeImmutable($when, new \DateTimeZone('UTC'));
    }

    public function testReadsAStreamsPosition(): void
    {
        $this->insert(['gtid_set' => 'uuid:1-4211']);

        $stream = $this->reader()->stream('orders_to_es');

        self::assertNotNull($stream);
        self::assertSame('orders_to_es', $stream->stream);
        self::assertSame('uuid:1-4211', $stream->position);
        self::assertTrue($stream->snapshotDone);
    }

    public function testAnUnknownStreamIsNullRatherThanAnError(): void
    {
        self::assertNull($this->reader()->stream('never_configured'));
    }

    public function testStreamsAreOrderedByName(): void
    {
        $this->insert(['stream' => 'zebra']);
        $this->insert(['stream' => 'alpha']);
        $this->insert(['stream' => 'middle']);

        $names = array_map(static fn ($s): string => $s->stream, $this->reader()->streams());

        self::assertSame(['alpha', 'middle', 'zebra'], $names);
    }

    public function testLagIsMeasuredFromTheSourcesTimestamp(): void
    {
        // 2026-08-11 12:00:00 UTC
        $this->insert(['last_event_ts_ms' => 1786449600000]);

        $stream = $this->reader()->stream('orders_to_es');

        self::assertNotNull($stream);
        self::assertEqualsWithDelta(2.5, $stream->lagSeconds(self::at('2026-08-11 12:00:02.5')), 0.01);
    }

    /**
     * A stream that has applied nothing is not the same as one that is caught up, and a
     * dashboard showing both as zero hides the difference that matters.
     */
    public function testLagIsUnknownBeforeAnyChangeIsApplied(): void
    {
        $this->insert(['last_event_ts_ms' => 0]);

        $stream = $this->reader()->stream('orders_to_es');

        self::assertNotNull($stream);
        self::assertNull($stream->lagSeconds());
        self::assertNull($stream->lastEventAt);
    }

    /** A source clock reading ahead of ours is not negative lag. */
    public function testLagIsNeverNegative(): void
    {
        $this->insert(['last_event_ts_ms' => 1786449600000]);

        $stream = $this->reader()->stream('orders_to_es');

        self::assertNotNull($stream);
        self::assertSame(0.0, $stream->lagSeconds(self::at('2026-08-11 11:59:00')));
    }

    /** A quiet table produces no changes, so silence alone cannot mean unhealthy. */
    public function testAStreamWithNoChangesYetIsHealthy(): void
    {
        $this->insert(['last_event_ts_ms' => 0]);

        self::assertTrue($this->reader()->isHealthy(60.0));
    }

    public function testAStreamThatHasFallenBehindIsUnhealthy(): void
    {
        $this->insert(['last_event_ts_ms' => 1786449600000]);

        $now = self::at('2026-08-11 12:10:00');

        self::assertFalse($this->reader()->isHealthy(60.0, $now));
        self::assertCount(1, $this->reader()->unhealthy(60.0, $now));
        self::assertTrue($this->reader()->isHealthy(3600.0, $now));
    }

    public function testARecordedErrorMakesAStreamUnhealthy(): void
    {
        $this->insert(['last_error' => 'elasticsearch: 429 rejected']);

        $stream = $this->reader()->stream('orders_to_es');

        self::assertNotNull($stream);
        self::assertSame('elasticsearch: 429 rejected', $stream->lastError);
        self::assertFalse($stream->isHealthy());
    }

    public function testNoStreamsIsHealthyRatherThanFailing(): void
    {
        self::assertTrue($this->reader()->isHealthy());
        self::assertSame([], $this->reader()->unhealthy());
    }

    public function testSnapshotProgressIsReportedWhileScanning(): void
    {
        $this->insert([
            'snapshot_done' => 0,
            'snapshot_rows_done' => 250,
            'snapshot_rows_total' => 1000,
        ]);

        $stream = $this->reader()->stream('orders_to_es');

        self::assertNotNull($stream);
        self::assertSame(0.25, $stream->snapshotProgress());
        self::assertStringContainsString('25%', $stream->describe());
    }

    /**
     * The estimate comes from table statistics and is sometimes zero. Inventing a
     * percentage from it would be worse than admitting there is none.
     */
    public function testSnapshotProgressIsUnknownWithoutAnEstimate(): void
    {
        $this->insert([
            'snapshot_done' => 0,
            'snapshot_rows_done' => 400,
            'snapshot_rows_total' => 0,
        ]);

        $stream = $this->reader()->stream('orders_to_es');

        self::assertNotNull($stream);
        self::assertNull($stream->snapshotProgress());
        self::assertStringContainsString('400 rows read', $stream->describe());
    }

    /** An estimate can be exceeded, since the table keeps growing during a scan. */
    public function testSnapshotProgressIsCappedAtComplete(): void
    {
        $this->insert([
            'snapshot_done' => 0,
            'snapshot_rows_done' => 1500,
            'snapshot_rows_total' => 1000,
        ]);

        $stream = $this->reader()->stream('orders_to_es');

        self::assertNotNull($stream);
        self::assertSame(1.0, $stream->snapshotProgress());
    }

    public function testACompletedSnapshotReportsFullProgress(): void
    {
        $this->insert(['snapshot_done' => 1, 'snapshot_rows_done' => 900, 'snapshot_rows_total' => 1000]);

        $stream = $this->reader()->stream('orders_to_es');

        self::assertNotNull($stream);
        self::assertSame(1.0, $stream->snapshotProgress());
        self::assertStringContainsString('streaming', $stream->describe());
    }

    /**
     * Fields are added over time and never repurposed, but a reader that guessed at an
     * unknown layout would report progress it has no basis for.
     */
    public function testARowFromANewerChangeflowIsRefused(): void
    {
        $this->insert(['schema_version' => Status::SUPPORTED_SCHEMA_VERSION + 1]);

        $this->expectException(SchemaVersionMismatch::class);
        $this->expectExceptionMessageMatches('/upgrade changeflow/');

        $this->reader()->stream('orders_to_es');
    }

    /** The table name reaches SQL, where an identifier cannot be bound as a parameter. */
    public function testAnUnsafeTableNameIsRefused(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        new Status($this->pdo, 'checkpoints; DROP TABLE users');
    }

    public function testAQualifiedTableNameIsAccepted(): void
    {
        $status = new Status($this->pdo, 'changeflow_meta.changeflow_checkpoints');

        self::assertInstanceOf(Status::class, $status);
    }

    /** changeflow stores UTC, so a reader in another zone must not shift the value. */
    public function testTimestampsAreReadAsUTC(): void
    {
        $this->insert(['updated_at' => '2026-08-11 12:00:00']);

        $stream = $this->reader()->stream('orders_to_es');

        self::assertNotNull($stream);
        self::assertSame('2026-08-11T12:00:00+00:00', $stream->updatedAt->format('c'));
    }
}
