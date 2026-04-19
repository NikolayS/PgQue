-- logres_uninstall.sql -- Remove logres from database
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$ begin
    perform logres.stop();
exception when others then
    null;
end $$;

drop schema if exists logres cascade;

-- Roles are database-global and may be shared across databases.
-- Do not drop them automatically here.
