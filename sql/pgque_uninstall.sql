-- pgque_uninstall.sql -- Remove pgque from database
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$ begin
    perform pgque.stop();
exception when others then
    null;
end $$;

drop schema if exists pgque cascade;

-- Roles are database-global and may be shared across databases.
-- Do not drop them automatically here.
