-- producer.sql -- keyed pgbench producer for the partition-keys read-amp bench.
--
-- Zipfian tenant skew (s=1.1) over 2000 tenants models a high-volume multi-tenant
-- profile: a heavy head of hot buckets plus a long tail. Each send carries a
-- ~200-byte JSON payload and a partition key of 'tenant-<n>'. The key rides
-- ev_extra1 (SPEC D1) so every slot's server-side hash filter routes it.
--
-- @QUEUE@ is rendered to the live queue name by run_bench.sh (pgbench has no
-- string-literal variable substitution, so the runner seds this placeholder).
\set tenant random_zipfian(1, 2000, 1.1)
select pgque.send(
    '@QUEUE@',
    'StorageObjectCreated',
    json_build_object(
        'tenant', 'tenant-' || :tenant,
        'bucket', 'bkt-' || (:tenant % 32),
        'object', md5(random()::text) || '/' || md5(random()::text) || '.bin',
        'size_bytes', (random() * 10485760)::bigint,
        'content_type', 'application/octet-stream',
        'etag', md5(random()::text),
        'created_at', clock_timestamp()
    )::text,
    'tenant-' || :tenant
);
