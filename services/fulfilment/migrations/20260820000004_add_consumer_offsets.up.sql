-- Local offset ledger standing in for a broker-managed consumer-group
-- commit (rskafka has no consumer-group protocol; see
-- docs/adr/0006-inbox-consumer-offset-ledger.md).

create table consumer_offsets (
  consumer_name text not null,
  topic text not null,
  partition int not null,
  next_offset bigint not null,
  updated_at timestamptz not null,
  primary key (consumer_name, topic, partition)
);
