-- Local offset ledger standing in for a broker-managed consumer-group
-- commit, since this project's Kafka client (rskafka) has no consumer
-- group protocol (see docs/adr/0006-inbox-consumer-offset-ledger.md).
-- `next_offset` is the offset to resume fetching from; it is only ever
-- advanced after the corresponding handler's DB transaction commits, or
-- after a DLQ publish is acknowledged for a poison message.

create table consumer_offsets (
  consumer_name text not null,
  topic text not null,
  partition int not null,
  next_offset bigint not null,
  updated_at timestamptz not null,
  primary key (consumer_name, topic, partition)
);
