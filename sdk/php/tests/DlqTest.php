<?php

declare(strict_types=1);

namespace Changeflow\Tests;

use Changeflow\Dlq;
use PHPUnit\Framework\TestCase;

final class DlqTest extends TestCase
{
    private string $directory;

    protected function setUp(): void
    {
        $this->directory = sys_get_temp_dir() . '/changeflow-dlq-' . bin2hex(random_bytes(4));
        mkdir($this->directory, 0o755, true);
    }

    protected function tearDown(): void
    {
        foreach (glob($this->directory . '/*') ?: [] as $file) {
            unlink($file);
        }
        rmdir($this->directory);
    }

    /** @param array<int, array<string, mixed>> $records */
    private function writeFile(string $name, array $records): void
    {
        $lines = array_map(
            static fn (array $r): string => json_encode($r, JSON_THROW_ON_ERROR),
            $records,
        );
        file_put_contents($this->directory . '/' . $name, implode("\n", $lines) . "\n");
    }

    /** @return array<string, mixed> */
    private static function record(string $key, array $overrides = []): array
    {
        return array_merge([
            'recorded_at' => '2026-08-11T12:00:00Z',
            'stream' => 'orders_to_es',
            'key' => $key,
            'version' => 1786449600123,
            'status' => 400,
            'reason' => 'mapper_parsing_exception: failed to parse field [total]',
            'body_bytes' => 84,
        ], $overrides);
    }

    public function testReadsRecords(): void
    {
        $this->writeFile('orders_to_es.jsonl', [self::record('42'), self::record('43')]);

        $records = (new Dlq($this->directory))->read($this->directory . '/orders_to_es.jsonl');

        self::assertCount(2, $records);
        self::assertSame('42', $records[0]->key);
        self::assertSame('orders_to_es', $records[0]->stream);
        self::assertSame(400, $records[0]->status);
        self::assertStringContainsString('mapper_parsing_exception', $records[0]->reason);
        // The version is kept so a replay can preserve the document's place in the order.
        self::assertSame(1786449600123, $records[0]->version);
        self::assertSame('2026-08-11T12:00:00+00:00', $records[0]->recordedAt->format('c'));
    }

    /**
     * These records exist because something was already lost once, so a line that cannot
     * be parsed must not be quietly skipped.
     */
    public function testACorruptLineIsAnError(): void
    {
        file_put_contents(
            $this->directory . '/orders_to_es.jsonl',
            json_encode(self::record('42')) . "\nnot json\n",
        );

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessageMatches('/line 2/');

        (new Dlq($this->directory))->read($this->directory . '/orders_to_es.jsonl');
    }

    public function testBlankLinesAreIgnored(): void
    {
        file_put_contents(
            $this->directory . '/orders_to_es.jsonl',
            json_encode(self::record('42')) . "\n\n" . json_encode(self::record('43')) . "\n",
        );

        self::assertCount(2, (new Dlq($this->directory))->read($this->directory . '/orders_to_es.jsonl'));
    }

    /** Row values can hold personal data, so the body is absent unless asked for. */
    public function testABodyIsOptional(): void
    {
        $this->writeFile('orders_to_es.jsonl', [
            self::record('42'),
            self::record('43', ['body' => ['id' => 43, 'status' => 'paid']]),
        ]);

        $records = (new Dlq($this->directory))->read($this->directory . '/orders_to_es.jsonl');

        self::assertNull($records[0]->body);
        self::assertNotNull($records[1]->body);
        self::assertStringContainsString('"status":"paid"', (string) $records[1]->body);
        // The size is recorded either way, since a refusal caused by document size cannot
        // be diagnosed without it.
        self::assertSame(84, $records[0]->bodyBytes);
    }

    public function testFindsRotatedFilesForAStream(): void
    {
        $this->writeFile('orders_to_es.jsonl', [self::record('1')]);
        $this->writeFile('orders_to_es.jsonl.20260811T120000.000000000', [self::record('2')]);
        $this->writeFile('users_to_es.jsonl', [self::record('3', ['stream' => 'users_to_es'])]);

        $dlq = new Dlq($this->directory);

        self::assertCount(2, $dlq->files('orders_to_es'));
        self::assertCount(3, $dlq->files());
    }

    /** Any value above zero means documents that are not in the destination. */
    public function testCountsAcrossRotatedFiles(): void
    {
        $this->writeFile('orders_to_es.jsonl', [self::record('1'), self::record('2')]);
        $this->writeFile('orders_to_es.jsonl.20260811T120000.000000000', [self::record('3')]);

        self::assertSame(3, (new Dlq($this->directory))->count('orders_to_es'));
    }

    public function testCountIsZeroWhenNothingHasBeenRefused(): void
    {
        self::assertSame(0, (new Dlq($this->directory))->count());
        self::assertSame([], (new Dlq($this->directory))->files());
    }

    public function testAMissingFileIsAnError(): void
    {
        $this->expectException(\RuntimeException::class);

        (new Dlq($this->directory))->read($this->directory . '/absent.jsonl');
    }

    public function testADeleteIsDistinguishable(): void
    {
        $this->writeFile('orders_to_es.jsonl', [self::record('42', ['deleted' => true])]);

        $records = (new Dlq($this->directory))->read($this->directory . '/orders_to_es.jsonl');

        self::assertTrue($records[0]->deleted);
    }
}
