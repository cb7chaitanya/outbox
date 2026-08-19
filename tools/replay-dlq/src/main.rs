use messaging::{Consumer, Producer, RskafkaConsumer, RskafkaProducer};
use persistence::dlq::DlqRecord;

fn usage() -> anyhow::Error {
    anyhow::anyhow!("usage: replay-dlq <source-topic> <dlq-offset> [broker]")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let source_topic = args.next().ok_or_else(usage)?;
    let offset: i64 = args.next().ok_or_else(usage)?.parse()?;
    let broker = args
        .next()
        .or_else(|| std::env::var("REDPANDA_BROKER").ok())
        .unwrap_or_else(|| "localhost:19092".to_string());
    if args.next().is_some() {
        return Err(usage());
    }

    let consumer = RskafkaConsumer::connect(vec![broker.clone()]).await?;
    let producer = RskafkaProducer::connect(vec![broker]).await?;
    let dlq_topic = persistence::dlq::dlq_topic(&source_topic);
    let record = consumer
        .fetch(&dlq_topic, offset, 1_000)
        .await?
        .into_iter()
        .find(|record| record.offset == offset)
        .ok_or_else(|| anyhow::anyhow!("no DLQ record at offset {offset}"))?;
    let dlq: DlqRecord = serde_json::from_slice(
        record
            .value
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("DLQ record has no value"))?,
    )?;
    if dlq.original_topic != source_topic {
        anyhow::bail!(
            "DLQ record belongs to {}, not {}",
            dlq.original_topic,
            source_topic
        );
    }
    let envelope = dlq.envelope.ok_or_else(|| {
        anyhow::anyhow!("DLQ record has no parseable envelope; correct it before replay")
    })?;
    let event_id = envelope
        .get("event_id")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");
    let replay_count = dlq.replay_count.saturating_add(1);
    producer
        .publish(
            &source_topic,
            dlq.original_key.as_deref().unwrap_or_default(),
            serde_json::to_vec(&envelope)?,
            vec![
                ("replayed_from_dlq".to_string(), dlq_topic.into_bytes()),
                (
                    "replay_count".to_string(),
                    replay_count.to_string().into_bytes(),
                ),
                ("event_id".to_string(), event_id.as_bytes().to_vec()),
            ],
        )
        .await?;
    println!("replayed event_id={event_id} to {source_topic} replay_count={replay_count}");
    Ok(())
}
