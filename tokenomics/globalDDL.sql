-- SPDX-License-Identifier: ice License 1.0

CREATE TABLE IF NOT EXISTS mining_boost_accepted_transactions (
                                                   created_at                             TIMESTAMP NOT NULL,
                                                   mining_boost_level                     SMALLINT NOT NULL,
                                                   tenant                                 TEXT NOT NULL,
                                                   tx_hash                                TEXT UNIQUE NOT NULL,
                                                   ice_amount                             TEXT NOT NULL,
                                                   payment_address                        TEXT NOT NULL DEFAULT '0x000000000000000000000000000000000000dead',
                                                   sender_address                         TEXT NOT NULL,
                                                   user_id                                TEXT NOT NULL,
                                            primary key(user_id,tx_hash));
ALTER TABLE mining_boost_accepted_transactions ADD COLUMN IF NOT EXISTS payment_address TEXT NOT NULL DEFAULT '0x000000000000000000000000000000000000dead';