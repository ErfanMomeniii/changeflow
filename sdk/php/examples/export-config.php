<?php

declare(strict_types=1);

/**
 * Emits changeflow's configuration from an application's own knowledge of its documents.
 *
 * Run it in CI and pass the result to `changeflow validate`, so a change to the model
 * that breaks the export fails the build instead of production.
 */

require __DIR__ . '/../vendor/autoload.php';

use Changeflow\ConfigBuilder;

// In a real application this comes from the model, a mapper, or whatever already knows
// which fields belong in the search document.
$searchDocumentFields = ['id', 'user_id', 'status', 'total_amount', 'placed_at'];

echo (new ConfigBuilder())
    ->source('${MYSQL_DSN}', 1001)
    ->checkpoint('${META_DSN}')
    ->runtime(['buffer_size' => 4096])
    ->elasticsearchStream(
        name: 'orders_to_es',
        table: 'shop.orders',
        index: 'orders-v1',
        addresses: ['${ES_URL}'],
        key: ['id'],
        include: $searchDocumentFields,
        rename: ['total_amount' => 'total'],
        alias: 'orders',
    )
    ->toYaml();
