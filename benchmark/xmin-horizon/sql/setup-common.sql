-- bystander table: an unrelated table sharing the same PG instance,
-- used to measure application-query latency while the queue workload runs.

drop table if exists bystander;
create table bystander (
  id   bigint primary key,
  payload text not null
);

insert into bystander
select i, repeat('x', 256)
from generate_series(1, 1000000) g(i);

analyze bystander;
