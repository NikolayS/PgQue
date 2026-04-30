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
    -- Owner of this function (the install owner / SECURITY DEFINER principal).
    v_maint_owner name;
    v_func_owner  name;
    v_func_oid    oid;
begin
    -- Resolve the owner of maint() itself once per call.
    select rolname into v_maint_owner
    from pg_proc p
    join pg_authid a on a.oid = p.proowner
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'pgque'
      and p.proname = 'maint'
      and pg_catalog.pg_get_function_arguments(p.oid) = '';

    for f in select func_name, func_arg from pgque.maint_operations()
    loop
        if f.func_name = 'pgque.maint_rotate_tables_step2' then
            continue;
        elsif f.func_name = 'vacuum' then
            continue;
        elsif f.func_arg is not null then
            -- Validate queue_extra_maint entries before executing.
            -- 1. Resolve the function name to a regprocedure (validates it exists
            --    and is unambiguous). Use a text->regprocedure cast via dynamic SQL
            --    so an invalid name raises an exception we can catch.
            begin
                execute format('select %L::regprocedure', f.func_name || '(text)')
                into v_func_oid;
            exception when others then
                raise warning 'pgque.maint: skipping queue_extra_maint entry % '
                    '— cannot resolve to regprocedure: %', f.func_name, sqlerrm;
                continue;
            end;

            -- 2. Check ownership: the extra-maint function must be owned by the
            --    same role that owns maint() (the install owner).
            select rolname into v_func_owner
            from pg_proc p
            join pg_authid a on a.oid = p.proowner
            where p.oid = v_func_oid;

            if v_func_owner is distinct from v_maint_owner then
                raise warning 'pgque.maint: skipping queue_extra_maint entry % '
                    '— owner (%) does not match maint() owner (%), refusing to '
                    'execute under SECURITY DEFINER', f.func_name, v_func_owner, v_maint_owner;
                continue;
            end if;

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

-- maint() is admin-level; revoke PUBLIC EXECUTE (PostgreSQL default) and
-- grant only to pgque_admin.
revoke execute on function pgque.maint() from public;
grant  execute on function pgque.maint() to pgque_admin;
