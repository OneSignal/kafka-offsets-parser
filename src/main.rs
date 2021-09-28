#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
use std::io::Write;
use std::u32;
use nom::number::complete::{be_u16, be_i32, be_u64, be_u32, be_i64};
use nom::multi::length_data;
use nom::sequence::tuple;
use nom::error::{ErrorKind, ParseError};
use nom::Err::Error;

use clap::{App, Arg};
use log::{info, self};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::util::get_rdkafka_version;
use rdkafka::Message;

type IResult<I, O> = nom::IResult<I, O, KafkaParseError<I>>;

// A context can be used to change the behavior of producers and consumers by adding callbacks
// that will be executed by librdkafka.
// This particular context sets up custom callbacks to log rebalancing events.
struct CustomContext;

impl ClientContext for CustomContext {}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        log::trace!("Pre rebalance {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        log::trace!("Post rebalance {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        log::trace!("Committing offsets: {:?}", result);
    }
}

// A type alias with your custom consumer can be created for convenience.
type LoggingConsumer = StreamConsumer<CustomContext>;

#[derive(Debug)]
struct OffsetCommitValue<'a> {
    version: u16,
    offset: u64,
    leader_epoch: Option<u32>,
    metadata: &'a str,
    commit_timestamp: u64,
    expire_timestamp: Option<u64>,
}

#[derive(Debug)]
enum KafkaParseError<I> {
    FromUtf8Error(std::str::Utf8Error),
    Nom(I, ErrorKind)
}

impl<I> ParseError<I> for KafkaParseError<I> {
  fn from_error_kind(input: I, kind: ErrorKind) -> Self {
    KafkaParseError::Nom(input, kind)
  }

  fn append(_: I, _: ErrorKind, other: Self) -> Self {
    other
  }
}


fn length_str(bytes: &[u8]) -> IResult<&[u8], &str> {
    let (bytes, sbuf) = length_data(be_u16)(bytes)?;
    match std::str::from_utf8(sbuf) {
        Ok(s) => Ok((bytes, s)),
        Err(e) => Err(Error(KafkaParseError::FromUtf8Error(e)))
    }
}

fn parse_offset_commit_value0(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue> {
    let (bytes, (offset, metadata, commit_timestamp)) = tuple((be_u64, length_str, be_u64))(bytes)?;
    Ok((bytes, OffsetCommitValue {
        version: 0,
        offset,
        metadata,
        commit_timestamp,
        expire_timestamp: None,
        leader_epoch: None,
    }))
}

fn parse_offset_commit_value1(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue> {
    let (bytes, (offset, metadata, commit_timestamp, expire_timestamp)) = tuple((be_u64, length_str, be_u64, be_u64))(bytes)?;
    Ok((bytes, OffsetCommitValue {
        version: 1,
        offset,
        metadata,
        commit_timestamp,
        expire_timestamp: Some(expire_timestamp),
        leader_epoch: None,
    }))
}

fn parse_offset_commit_value3(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue> {
    let (bytes, (offset, leader_epoch, metadata, commit_timestamp)) = tuple((be_u64, be_u32, length_str, be_u64))(bytes)?;
    Ok((bytes, OffsetCommitValue {
        version: 3,
        offset,
        metadata,
        commit_timestamp,
        leader_epoch: if leader_epoch == u32::MAX { None } else { Some(leader_epoch) },
        expire_timestamp: None,
    }))
}

fn parse_offset_commit_value(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue> {
    let (bytes, version) = be_u16(bytes)?;
    match version {
        0     => parse_offset_commit_value0(bytes),
        1..=2 => parse_offset_commit_value1(bytes),
        3     => parse_offset_commit_value3(bytes),
        _ => Err(Error(KafkaParseError::Nom(bytes, ErrorKind::Fail)))
    }
}

#[derive(Debug)]
struct OffsetKey<'a> {
    group: &'a str,
    topic: &'a str,
    partition: i32,
}

fn parse_offset_key(bytes: &[u8]) -> IResult<&[u8], Option<OffsetKey>> {
    let (bytes, (group, topic, partition)) = tuple((length_str, length_str, be_i32))(bytes)?;

    Ok((bytes, Some(OffsetKey { group, topic, partition })))
}

fn parse_maybe_offset_key(bytes: &[u8]) -> IResult<&[u8], Option<OffsetKey>> {
    let (bytes, version) = be_u16(bytes)?;
    match version {
        0..=1 => parse_offset_key(bytes),
        _ => Ok((bytes, None))
    }
}

async fn consume_and_print(brokers: &str, group_id: &str) {
    let context = CustomContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "latest")
        //.set("statistics.interval.ms", "30000")
        //.set("auto.offset.reset", "smallest")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    eprintln!("subscribing");
    consumer
        .subscribe(&["__consumer_offsets"])
        .expect("Can't subscribe to specified topics");

    loop {
        match consumer.recv().await {
            Err(e) => eprintln!("Kafka error: {}", e),
            Ok(m) => {
                if let Some(key) = m.key() {
                    if let (_, Some(offset_key)) = parse_maybe_offset_key(key).unwrap() {
                        println!("{:?}", offset_key);
                        // Integers are big endian
                        if let Some(bytes) = m.payload() {
                            let (_, message) = parse_offset_commit_value(bytes).unwrap();
                            println!("{:?}", message);
                        }
                    }
                }
            }
        };
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let matches = App::new("consumer example")
        .version(option_env!("CARGO_PKG_VERSION").unwrap_or(""))
        .about("Parse the __consumer_offsets topic messages")
        .arg(
            Arg::with_name("brokers")
                .short("b")
                .long("brokers")
                .help("Broker list in kafka format")
                .takes_value(true)
                .default_value("localhost:9092"),
        )
        .arg(
            Arg::with_name("group-id")
                .short("g")
                .long("group-id")
                .help("Consumer group id")
                .takes_value(true)
                .default_value("consumer-offsets-parser"),
        )
        .get_matches();

    let (version_n, version_s) = get_rdkafka_version();
    info!("rd_kafka_version: 0x{:08x}, {}", version_n, version_s);

    let brokers = matches.value_of("brokers").unwrap();
    let group_id = matches.value_of("group-id").unwrap();

    consume_and_print(brokers, group_id).await
}
