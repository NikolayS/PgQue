-- pgque-api/types.sql -- Shared public types
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- pgque.message type (idempotent creation)
do $$ begin
    create type pgque.message as (
        msg_id      bigint,
        batch_id    bigint,
        type        text,
        payload     text,
        retry_count int4,
        created_at  timestamptz,
        extra1      text,
        extra2      text,
        extra3      text,
        extra4      text
    );
exception when duplicate_object then null;
end $$;
