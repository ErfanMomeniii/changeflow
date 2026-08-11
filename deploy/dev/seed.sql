-- Local development seed.
--
-- The orders table deliberately exercises the awkward corners of the type map:
-- an unsigned BIGINT above 2^63, an exact DECIMAL, ENUM and SET (which travel as
-- integers in the binlog), DATETIME versus TIMESTAMP, JSON, and a latin1 column.
-- Decoding any of them incorrectly is the kind of bug that stays invisible until
-- someone compares values by hand.

CREATE USER IF NOT EXISTS 'cdc'@'%' IDENTIFIED BY 'cdc';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
GRANT SELECT ON `shop`.* TO 'cdc'@'%';

-- The checkpoint store lives in its own database with its own grants, so the
-- replication user can never write and the checkpoint user can never read rows.
CREATE DATABASE IF NOT EXISTS changeflow_meta;
GRANT SELECT, INSERT, UPDATE, DELETE ON `changeflow_meta`.* TO 'cdc'@'%';
FLUSH PRIVILEGES;

USE `shop`;

CREATE TABLE IF NOT EXISTS `orders` (
    `id`             BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `user_id`        BIGINT UNSIGNED NOT NULL,
    `status`         ENUM('draft','paid','shipped','cancelled') NOT NULL DEFAULT 'draft',
    `channels`       SET('web','ios','android','pos') NOT NULL DEFAULT 'web',
    `total_amount`   DECIMAL(18,2) NOT NULL DEFAULT 0.00,
    `is_gift`        TINYINT(1) NOT NULL DEFAULT 0,
    `note_latin1`    VARCHAR(64) CHARACTER SET latin1 NULL,
    `metadata`       JSON NULL,
    `placed_at`      DATETIME(3) NULL,
    `updated_at`     TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    `internal_note`  TEXT NULL,
    PRIMARY KEY (`id`),
    KEY `idx_user` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- A composite-key table, to prove key joining and escaping.
CREATE TABLE IF NOT EXISTS `order_items` (
    `order_id`  BIGINT UNSIGNED NOT NULL,
    `sku`       VARCHAR(64) NOT NULL,
    `qty`       INT UNSIGNED NOT NULL DEFAULT 1,
    `unit_price` DECIMAL(18,2) NOT NULL DEFAULT 0.00,
    PRIMARY KEY (`order_id`, `sku`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- A table with no primary key. Its rows have no stable identity, so it cannot be
-- replicated idempotently; it exists to check that this is reported rather than
-- papered over by guessing a column.
CREATE TABLE IF NOT EXISTS `audit_log` (
    `actor`  VARCHAR(32) NOT NULL,
    `action` VARCHAR(32) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Pre-existing rows. These produce no binlog events, so a reader that only tails
-- the binlog never learns they exist; they arrive only via a table scan.
INSERT INTO `orders` (`id`, `user_id`, `status`, `channels`, `total_amount`, `note_latin1`, `metadata`, `placed_at`)
VALUES
    (1, 7,  'paid',    'web,ios',  19.90, 'café',   '{"coupon":"WELCOME"}', '2023-01-15 10:30:00.000'),
    (2, 7,  'shipped', 'pos',     249.99, NULL,     NULL,                   '2023-06-02 18:04:11.250'),
    -- id above 2^63: a signed 64-bit read of this value is negative.
    (18446744073709551000, 42, 'draft', 'android', 0.01, NULL, '{"stress":true}', NULL);

INSERT INTO `order_items` (`order_id`, `sku`, `qty`, `unit_price`) VALUES
    (1, 'SKU-1', 2, 9.95),
    (1, 'SKU:2', 1, 0.00),  -- a colon in the key, to prove key escaping
    (2, 'SKU-3', 1, 249.99);
