-- SPDX-License-Identifier: ice License 1.0

CREATE TABLE IF NOT EXISTS mining_boost_accepted_transactions (
                                                   created_at                             TIMESTAMP NOT NULL,
                                                   mining_boost_level                     SMALLINT NOT NULL,
                                                   tenant                                 TEXT,
                                                   tx_hash                                TEXT UNIQUE,
                                                   ice_amount                             TEXT,
                                                   sender_address                         TEXT,
                                                   user_id                                TEXT,
                                            primary key(user_id,tx_hash));