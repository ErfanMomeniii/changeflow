<?php

declare(strict_types=1);

namespace Changeflow\Tests;

use Changeflow\ConfigBuilder;
use PHPUnit\Framework\TestCase;

final class ConfigBuilderTest extends TestCase
{
    private function builder(): ConfigBuilder
    {
        return (new ConfigBuilder())
            ->source('${MYSQL_DSN}', 1001)
            ->checkpoint('${META_DSN}');
    }

    public function testBuildsAnElasticsearchStream(): void
    {
        $yaml = $this->builder()
            ->elasticsearchStream(
                name: 'orders_to_es',
                table: 'shop.orders',
                index: 'orders-v1',
                addresses: ['${ES_URL}'],
                key: ['id'],
                include: ['id', 'status', 'total_amount'],
                rename: ['total_amount' => 'total'],
                alias: 'orders',
            )
            ->toYaml();

        foreach ([
            'source:',
            'dsn: "${MYSQL_DSN}"',
            'server_id: 1001',
            'streams:',
            '  orders_to_es:',
            'table: "shop.orders"',
            'type: "elasticsearch"',
            'index: "orders-v1"',
            'alias: "orders"',
            'key: ["id"]',
            'total_amount: "total"',
        ] as $expected) {
            self::assertStringContainsString($expected, $yaml);
        }
    }

    public function testBuildsAClickHouseStream(): void
    {
        $yaml = $this->builder()
            ->clickHouseStream(
                name: 'orders_to_clickhouse',
                table: 'shop.orders',
                dsn: '${CH_DSN}',
                destinationTable: 'analytics.orders',
                key: ['id'],
            )
            ->toYaml();

        self::assertStringContainsString('type: "clickhouse"', $yaml);
        self::assertStringContainsString('table: "analytics.orders"', $yaml);
    }

    /**
     * A regenerated file should produce a reviewable diff, so the ordering cannot depend
     * on the order the application happened to declare its streams in.
     */
    public function testStreamsAreOrderedByName(): void
    {
        $yaml = $this->builder()
            ->elasticsearchStream('zebra', 'a.b', 'zebra-v1', ['x'], ['id'])
            ->elasticsearchStream('alpha', 'a.c', 'alpha-v1', ['x'], ['id'])
            ->toYaml();

        self::assertLessThan(
            strpos($yaml, 'zebra:'),
            strpos($yaml, 'alpha:'),
            'streams should be emitted in name order'
        );
    }

    public function testOutputIsStableBetweenRuns(): void
    {
        $make = fn (): string => $this->builder()
            ->elasticsearchStream('orders_to_es', 'shop.orders', 'orders-v1', ['${ES_URL}'], ['id'])
            ->toYaml();

        $first = $make();
        for ($i = 0; $i < 5; ++$i) {
            self::assertSame($first, $make());
        }
    }

    /**
     * A value carrying a colon or an environment reference would change the meaning of
     * the line if it were emitted bare.
     */
    public function testValuesAreQuoted(): void
    {
        $yaml = $this->builder()
            ->elasticsearchStream('orders_to_es', 'shop.orders', 'orders-v1', ['http://es:9200'], ['id'])
            ->toYaml();

        self::assertStringContainsString('["http://es:9200"]', $yaml);
    }

    /** The checkpoint table's column bounds this, and a longer name could never checkpoint. */
    public function testAnOverlongStreamNameIsRefused(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/at most 48/');

        $this->builder()->elasticsearchStream(str_repeat('s', 49), 'shop.orders', 'i', ['x'], ['id']);
    }

    public function testAStreamNameWithUnusableCharactersIsRefused(): void
    {
        $this->expectException(\InvalidArgumentException::class);

        $this->builder()->elasticsearchStream('orders to es', 'shop.orders', 'i', ['x'], ['id']);
    }

    public function testATableWithoutADatabaseIsRefused(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/database\.table/');

        $this->builder()->elasticsearchStream('orders_to_es', 'orders', 'i', ['x'], ['id']);
    }

    public function testADuplicateStreamNameIsRefused(): void
    {
        $builder = $this->builder()->elasticsearchStream('orders_to_es', 'shop.orders', 'i', ['x'], ['id']);

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/already defined/');

        $builder->elasticsearchStream('orders_to_es', 'shop.orders', 'j', ['x'], ['id']);
    }

    /** Without its key column, a row cannot be identified in the destination. */
    public function testIncludingColumnsButNotTheKeyIsRefused(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessageMatches('/key column id/');

        $this->builder()->elasticsearchStream(
            'orders_to_es',
            'shop.orders',
            'orders-v1',
            ['x'],
            key: ['id'],
            include: ['status', 'total_amount'],
        );
    }

    public function testASourceIsRequired(): void
    {
        $builder = (new ConfigBuilder())
            ->checkpoint('${META_DSN}')
            ->elasticsearchStream('orders_to_es', 'shop.orders', 'i', ['x'], ['id']);

        $this->expectException(\LogicException::class);

        $builder->toYaml();
    }

    public function testACheckpointStoreIsRequired(): void
    {
        $builder = (new ConfigBuilder())
            ->source('${MYSQL_DSN}', 1001)
            ->elasticsearchStream('orders_to_es', 'shop.orders', 'i', ['x'], ['id']);

        $this->expectException(\LogicException::class);

        $builder->toYaml();
    }

    public function testAtLeastOneStreamIsRequired(): void
    {
        $this->expectException(\LogicException::class);

        $this->builder()->toYaml();
    }

    public function testWritesToAFileCreatingTheDirectory(): void
    {
        $path = sys_get_temp_dir() . '/changeflow-test-' . bin2hex(random_bytes(4)) . '/deploy/changeflow.yaml';

        $this->builder()
            ->elasticsearchStream('orders_to_es', 'shop.orders', 'orders-v1', ['${ES_URL}'], ['id'])
            ->writeTo($path);

        self::assertFileExists($path);
        self::assertStringContainsString('orders_to_es:', (string) file_get_contents($path));

        unlink($path);
        rmdir(dirname($path));
        rmdir(dirname($path, 2));
    }

    /** The generated file is committed, so it says where it came from. */
    public function testOutputSaysItIsGenerated(): void
    {
        $yaml = $this->builder()
            ->elasticsearchStream('orders_to_es', 'shop.orders', 'orders-v1', ['x'], ['id'])
            ->toYaml();

        self::assertStringContainsString('# Generated by', $yaml);
        self::assertStringContainsString('changeflow validate', $yaml);
    }
}
