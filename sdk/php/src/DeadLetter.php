<?php

declare(strict_types=1);

namespace Changeflow;

/** One document a destination refused permanently. */
final readonly class DeadLetter
{
    public function __construct(
        public \DateTimeImmutable $recordedAt,
        public string $stream,
        public string $key,
        /** The version the document carried, so a replay can keep its place in the ordering. */
        public int $version,
        public bool $deleted,
        public int $status,
        public string $reason,
        public int $bodyBytes,
        /**
         * The document itself, present only when the deployment opted in: row values can
         * hold personal data, and a dead letter file travels more easily than a database.
         */
        public ?string $body,
    ) {
    }

    /** @param array<string, mixed> $record */
    public static function fromArray(array $record): self
    {
        $body = $record['body'] ?? null;

        return new self(
            recordedAt: new \DateTimeImmutable(
                (string) ($record['recorded_at'] ?? 'now'),
                new \DateTimeZone('UTC'),
            ),
            stream: (string) ($record['stream'] ?? ''),
            key: (string) ($record['key'] ?? ''),
            version: (int) ($record['version'] ?? 0),
            deleted: (bool) ($record['deleted'] ?? false),
            status: (int) ($record['status'] ?? 0),
            reason: (string) ($record['reason'] ?? ''),
            bodyBytes: (int) ($record['body_bytes'] ?? 0),
            body: $body === null ? null : json_encode($body, JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE),
        );
    }
}
