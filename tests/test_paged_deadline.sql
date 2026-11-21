-- Fixed reference times keep calendar deadline validation independent of CI date.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
\set ON_ERROR_STOP on
begin;
set local timezone = 'UTC';
do $$
declare
    v_case record;
    v_until timestamptz;
    v_rejected boolean;
    v_count int := 0;
begin
    for v_case in select * from (values
        ('nonleap February past', '2025-02-01'::timestamptz, interval '1 month -29 days', null::timestamptz),
        ('leap February equal', '2024-02-01', interval '1 month -29 days', null),
        ('nonleap February equal', '2025-02-01', interval '1 month -28 days', null),
        ('leap February future', '2024-02-01', interval '1 month -28 days', '2024-02-02'),
        ('nonleap January end past', '2025-01-31', interval '1 month -29 days', null),
        ('leap January end equal', '2024-01-31', interval '1 month -29 days', null),
        ('leap February end equal', '2024-02-29', interval '1 month -29 days', null),
        ('March end reverse month', '2025-03-31', interval '-1 month 30 days 1 second', null),
        ('normal calendar month', '2025-01-31', interval '1 month', '2025-02-28'),
        ('leap calendar month', '2024-01-31', interval '1 month', '2024-02-29'),
        ('calendar year', '2024-02-29', interval '1 year', '2025-02-28'),
        ('mixed valid calendar interval', '2025-02-01', interval '1 month -27 days', '2025-02-02'),
        ('fixed seconds', '2025-02-01', interval '1 second', '2025-02-01 00:00:01'),
        ('smallest duration', '2025-02-01', interval '1 microsecond', '2025-02-01 00:00:00.000001'),
        ('past with positive comparison', '2025-10-07', interval '-1 year 360 days 1 second', null),
        ('zero', '2025-02-01', interval '0', null),
        ('negative', '2025-02-01', interval '-1 second', null),
        ('null lease', '2025-02-01', null, null),
        ('null reference', null, interval '1 second', null),
        ('infinite reference', 'infinity', interval '1 second', null),
        ('negative infinite reference', '-infinity', interval '1 second', null),
        ('timestamp overflow', '2025-02-01', interval '178000000 years', null)
    ) as cases(label, reference_time, ttl, expected) loop
        v_rejected := false;
        begin
            v_until := pgque._lease_deadline(v_case.reference_time, v_case.ttl);
        exception when invalid_parameter_value then
            v_rejected := true;
        end;
        if v_case.expected is null then
            if not v_rejected then
                raise exception '%: invalid deadline accepted: %', v_case.label, v_until;
            end if;
        elsif v_rejected or v_until is distinct from v_case.expected then
            raise exception '%: expected %, got % (rejected=%)',
                v_case.label, v_case.expected, v_until, v_rejected;
        end if;
        v_count := v_count + 1;
    end loop;
    if has_function_privilege('pgque_reader', 'pgque._lease_deadline(timestamptz,interval)', 'execute')
        or has_function_privilege('pgque_writer', 'pgque._lease_deadline(timestamptz,interval)', 'execute') then
        raise exception 'deadline helper must stay private';
    end if;
    raise notice 'PASS: % fixed-reference deadline cases', v_count;
end $$;
commit;
