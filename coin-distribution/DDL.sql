-- SPDX-License-Identifier: ice License 1.0
DO $$ BEGIN
    CREATE DOMAIN uint256 AS NUMERIC(78,0) NOT NULL DEFAULT 0
    CHECK (VALUE >= 0 AND VALUE <= 115792089237316195423570985008687907853269984665640564039457584007913129639935)
    CHECK (SCALE(VALUE) = 0);
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'pending_coin_distributions_status') THEN
        create type pending_coin_distributions_status AS ENUM ('NEW', 'PENDING', 'ACCEPTED', 'REJECTED');
    END IF;
END
$$;


CREATE TABLE IF NOT EXISTS pending_coin_distributions  (
                    created_at                timestamp NOT NULL,
                    internal_id               bigint    NOT NULL,
                    day                       date      NOT NULL,
                    iceflakes                 uint256,
                    user_id                   text      NOT NULL,
                    eth_address               text      NOT NULL,
                    eth_status                pending_coin_distributions_status NOT NULL DEFAULT 'NEW',
                    eth_tx                    text,
                    PRIMARY KEY(day, user_id))
                    WITH (FILLFACTOR = 70);

CREATE INDEX IF NOT EXISTS pending_coin_distributions_worker_number_ix ON pending_coin_distributions (eth_status, (internal_id % 10), created_at ASC);
CREATE INDEX IF NOT EXISTS pending_coin_distributions_eth_status_tx_ix ON pending_coin_distributions (eth_status, eth_tx);
CREATE INDEX IF NOT EXISTS pending_coin_distributions_eth_status_ix ON pending_coin_distributions (eth_status);

CREATE TABLE IF NOT EXISTS global (
                    key       text NOT NULL primary key,
                    value     text NOT NULL )
                    WITH (FILLFACTOR = 70);
INSERT INTO global (key,value)
            VALUES ('coin_distributer_enabled','true'),
                   ('coin_collector_enabled','true'),
                   ('coin_collector_forced_execution','false'),
                   ('coin_collector_start_date','2023-12-20T10:54:20.657949659Z'),
                   ('coin_collector_end_date','2024-10-24T10:54:20.657949659Z'),
                   ('coin_collector_min_mining_streaks_required','0'),
                   ('coin_collector_start_hour','0'),
                   ('coin_collector_min_balance_required','0'),
                   ('coin_collector_denied_countries','')
         ON CONFLICT(key) DO NOTHING;

CREATE TABLE IF NOT EXISTS coin_distributions_by_earner (
                    created_at                timestamp NOT NULL,
                    internal_id               bigint    NOT NULL,
                    balance                   bigint    NOT NULL,
                    day                       date      NOT NULL,
                    username                  text      NOT NULL,
                    referred_by_username      text      NOT NULL,
                    user_id                   text      NOT NULL,
                    earner_user_id            text      NOT NULL,
                    eth_address               text      NOT NULL,
                    PRIMARY KEY(day, user_id, earner_user_id))
                    WITH (FILLFACTOR = 70);

CREATE TABLE IF NOT EXISTS coin_distributions_pending_review  (
                    created_at                timestamp NOT NULL,
                    internal_id               bigint    NOT NULL,
                    ice                       bigint    NOT NULL,
                    day                       date      NOT NULL,
                    iceflakes                 uint256           ,
                    username                  text      NOT NULL,
                    referred_by_username      text      NOT NULL,
                    user_id                   text      NOT NULL,
                    eth_address               text      NOT NULL,
                    PRIMARY KEY(day, user_id));

CREATE INDEX IF NOT EXISTS coin_distributions_pending_review_internal_id_ix ON coin_distributions_pending_review (internal_id);
CREATE INDEX IF NOT EXISTS coin_distributions_pending_review_created_at_ix ON coin_distributions_pending_review (created_at);
CREATE INDEX IF NOT EXISTS coin_distributions_pending_review_ice_ix ON coin_distributions_pending_review (ice);
CREATE INDEX IF NOT EXISTS coin_distributions_pending_review_username_ix ON coin_distributions_pending_review (username);
CREATE INDEX IF NOT EXISTS coin_distributions_pending_review_referred_by_username_ix ON coin_distributions_pending_review (referred_by_username);
CREATE INDEX IF NOT EXISTS coin_distributions_pending_review_lookup1_ix ON coin_distributions_pending_review (ice,internal_id);
CREATE INDEX IF NOT EXISTS coin_distributions_pending_review_lookup2_ix ON coin_distributions_pending_review (created_at,internal_id);
CREATE INDEX IF NOT EXISTS coin_distributions_pending_review_lookup3_ix ON coin_distributions_pending_review (username,internal_id);
CREATE INDEX IF NOT EXISTS coin_distributions_pending_review_lookup4_ix ON coin_distributions_pending_review (ice,username,internal_id);
CREATE INDEX IF NOT EXISTS coin_distributions_pending_review_lookup5_ix ON coin_distributions_pending_review (referred_by_username,internal_id);
CREATE INDEX IF NOT EXISTS coin_distributions_pending_review_lookup6_ix ON coin_distributions_pending_review (ice,referred_by_username,internal_id);

CREATE TABLE IF NOT EXISTS reviewed_coin_distributions  (
                    reviewed_at               timestamp NOT NULL,
                    created_at                timestamp NOT NULL,
                    internal_id               bigint    NOT NULL,
                    ice                       bigint    NOT NULL,
                    day                       date      NOT NULL,
                    review_day                date      NOT NULL,
                    iceflakes                 uint256           ,
                    username                  text      NOT NULL,
                    referred_by_username      text      NOT NULL,
                    user_id                   text      NOT NULL ,
                    eth_address               text      NOT NULL,
                    reviewer_user_id          text      NOT NULL,
                    decision                  text      NOT NULL,
                    PRIMARY KEY(user_id, day, review_day));

create or replace procedure approve_coin_distributions(reviewer_user_id text, nested boolean)
language plpgsql
    as $$
declare
         now timestamp := current_timestamp;
BEGIN
    insert into pending_coin_distributions(created_at, internal_id, day, iceflakes, user_id, eth_address)
    select created_at, internal_id, day, iceflakes, user_id, eth_address
    from coin_distributions_pending_review;

    insert into reviewed_coin_distributions(reviewed_at, created_at, internal_id, ice, day, review_day, iceflakes, username, referred_by_username, user_id, eth_address, reviewer_user_id, decision)
    select now, created_at, internal_id, ice, day, now::date, iceflakes, username, referred_by_username, user_id, eth_address, reviewer_user_id, 'approve'
    from coin_distributions_pending_review;

    delete from coin_distributions_pending_review where 1=1;

    IF nested is false THEN
        commit;
    END IF;
end; $$;

create or replace procedure deny_coin_distributions(reviewer_user_id text, nested boolean)
language plpgsql
    as $$
declare
         now timestamp := current_timestamp;
BEGIN
    insert into reviewed_coin_distributions(reviewed_at, created_at, internal_id, ice, day, review_day, iceflakes, username, referred_by_username, user_id, eth_address, reviewer_user_id, decision)
    select now, created_at, internal_id, ice, day, now::date, iceflakes, username, referred_by_username, user_id, eth_address, reviewer_user_id, 'deny'
    from coin_distributions_pending_review;

    delete from coin_distributions_pending_review where 1=1;

    INSERT INTO global (key,value)
                VALUES ('coin_distributer_enabled','false'),
                       ('coin_collector_enabled','false')
    ON CONFLICT (key) DO UPDATE
    			SET value = EXCLUDED.value;

    IF nested is false THEN
        commit;
    END IF;
end; $$;

create or replace procedure prepare_coin_distributions_for_review(nested boolean)
language plpgsql
    as $$
declare
         zeros text := '0000000000000000';
BEGIN
    delete from coin_distributions_by_earner WHERE balance = 0;

    insert into coin_distributions_pending_review(created_at, internal_id, ice, day, iceflakes, username, referred_by_username, user_id, eth_address)
        SELECT created_at, internal_id, ice, day, (ice::text||zeros)::uint256 AS iceflakes, username, referred_by_username, user_id, eth_address
        FROM (select
                   min (created_at) filter ( where user_id=earner_user_id )  AS created_at,
                   min (internal_id) filter ( where user_id=earner_user_id )  AS internal_id,
                   sum(balance) AS ice,
                   min (day) filter ( where user_id=earner_user_id ) AS day,
                   string_agg(username,'') AS username,
                   string_agg(referred_by_username,'') AS referred_by_username,
                   user_id,
                   string_agg(eth_address,'') AS eth_address
                from coin_distributions_by_earner
                group by day,user_id) AS X;

    delete from coin_distributions_by_earner where 1=1;

    IF nested is false THEN
        commit;
    END IF;
end; $$;