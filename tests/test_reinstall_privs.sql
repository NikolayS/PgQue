-- test_reinstall_privs.sql -- Verify internal table & helper revokes survive reinstall
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

/*
 * roles.sql grants ALL ON ALL TABLES / EXECUTE ON ALL FUNCTIONS in schema
 * pgque to pgque_admin before the pgque-api modules run. On reinstall, every
 * pgque-api internal table and SECURITY DEFINER helper must explicitly revoke
 * pgque_admin so privileges never escalate on re-running pgque.sql.
 */
do $$
declare
    v_tbl text;
    v_priv text;
    v_fn text;
    v_role text;
    v_oid oid;
begin
    /* partition_consumer and partition_slot: SELECT-only for pgque_admin, none for app roles. */
    foreach v_tbl in array array[
        'pgque.partition_consumer',
        'pgque.partition_slot'
    ] loop
        assert has_table_privilege('pgque_admin', v_tbl, 'SELECT'),
            'pgque_admin must retain SELECT on ' || v_tbl;

        foreach v_priv in array array['INSERT', 'UPDATE', 'DELETE', 'TRUNCATE'] loop
            assert not has_table_privilege('pgque_admin', v_tbl, v_priv),
                format('pgque_admin must NOT hold %s on %s', v_priv, v_tbl);
        end loop;

        foreach v_role in array array['public', 'pgque_reader', 'pgque_writer'] loop
            assert not has_table_privilege(v_role, v_tbl, 'SELECT'),
                format('%s must NOT hold SELECT on %s', v_role, v_tbl);
        end loop;
    end loop;

    /* Internal state tables: no direct access for any role (including pgque_admin). */
    foreach v_tbl in array array[
        'pgque.idem',
        'pgque.page_state'
    ] loop
        foreach v_role in array array['public', 'pgque_reader', 'pgque_writer', 'pgque_admin'] loop
            foreach v_priv in array array['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'TRUNCATE'] loop
                assert not has_table_privilege(v_role, v_tbl, v_priv),
                    format('%s must NOT hold %s on %s', v_role, v_priv, v_tbl);
            end loop;
        end loop;
    end loop;

    /* All pgque-api internal helpers: revoked from public, reader, writer, AND admin. */
    foreach v_fn in array array[
        'pgque._assert_unpaged(bigint)',
        'pgque._clear_member_cursor(integer,integer)',
        'pgque._clear_paged_active(integer,integer)',
        'pgque._event_retry_core(bigint,bigint,timestamp with time zone)',
        'pgque._is_partition_slot_consumer(integer,text)',
        'pgque._lease_deadline(timestamp with time zone,interval)',
        'pgque._lock_page(uuid)',
        'pgque._nack_batch_event(bigint,pgque.message,interval,text)',
        'pgque._nack_paged_event(bigint,pgque.message,interval,text)',
        'pgque._next_batch_coop(text,text,text,interval,integer,interval,interval,boolean)',
        'pgque._next_batch_custom(text,text,interval,integer,interval,boolean)',
        'pgque._page_failures(jsonb)',
        'pgque._page_messages(pgque.page_state,integer)',
        'pgque._partition_n_cap_guard()',
        'pgque._receive_page(bigint,text,text,integer,interval,text,integer,integer)',
        'pgque._slot_batch(text,text,integer,integer)',
        'pgque._slot_guard(text,text,integer,integer,text)',
        'pgque._slot_name(text,integer,integer)',
        'pgque._transfer_paged_active(integer,integer,integer,bigint)',
        'pgque._validate_coop_names(text,text,text)',
        'pgque._validate_page_args(text,text,text,integer,interval)',
        'pgque._validate_pending_page(pgque.page_state,uuid,text)',
        'pgque.insert_event_bulk(text,text,text[])'
    ] loop
        v_oid := to_regprocedure(v_fn);
        assert v_oid is not null, 'missing expected internal helper: ' || v_fn;

        foreach v_role in array array['public', 'pgque_reader', 'pgque_writer', 'pgque_admin'] loop
            assert not has_function_privilege(v_role, v_oid, 'EXECUTE'),
                format('%s must NOT hold EXECUTE on internal helper %s', v_role, v_fn);
        end loop;
    end loop;

    raise notice 'PASS: reinstall_privs - internal tables and helpers remain least-privilege';
end $$;
