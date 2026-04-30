-- pgque maint() -- default maintenance runner for v0.1
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Runs PgQ maintenance operations (rotation, retry, extra hooks).
-- Experimental addons may override this function to extend maintenance.

-- maint() runs rotation step1 and retry. Step2 needs its own transaction
-- (PgQ design requirement) and is scheduled separately by pgque.start().
create or replace function pgque.maint()
returns integer as $$
declare
    f record;
    r integer;
    total integer := 0;
begin
    for f in select func_name, func_arg from pgque.maint_operations()
    loop
        if f.func_name = 'pgque.maint_rotate_tables_step2' then
            continue;
        elsif f.func_name = 'vacuum' then
            continue;
        elsif f.func_arg is not null then
            execute 'select ' || f.func_name || '(' || quote_literal(f.func_arg) || ')' into r;
            total := total + r;
        else
            execute 'select ' || f.func_name || '()' into r;
            total := total + r;
        end if;
    end loop;

    return total;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
