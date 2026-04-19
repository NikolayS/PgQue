-- pg_current_uninstall.sql -- Remove pg_current from database
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$ begin
    perform pg_current.stop();
exception when others then
    null;
end $$;

drop schema if exists pg_current cascade;

-- Roles are database-global and may be shared across databases.
-- Do not drop them automatically here.
