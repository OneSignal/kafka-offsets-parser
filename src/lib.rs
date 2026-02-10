use std::borrow::Cow;
use std::convert::TryFrom;

use thiserror::Error;

use nom::bytes::complete::take;
use nom::number::complete::{be_u16, be_i32, be_u64, be_u32};
use nom::multi::length_data;
use nom::sequence::tuple;
use nom::error::{ErrorKind, ParseError};

type IResult<I, O> = nom::IResult<I, O, ConsumerOffsetsMessageParseError<I>>;

/// Error type for our parsers
#[derive(Error, Debug)]
pub enum ConsumerOffsetsMessageParseError<I: std::fmt::Debug> {
    #[error("invalid utf8 sequence")]
    FromUtf8Error(#[from] std::str::Utf8Error),
    #[error("parsing error")]
    Nom(I, ErrorKind),
    /// Ran out of data
    #[error("insufficient data")]
    Incomplete,
}

impl<I: std::fmt::Debug> ParseError<I> for ConsumerOffsetsMessageParseError<I> {
  fn from_error_kind(input: I, kind: ErrorKind) -> Self {
    ConsumerOffsetsMessageParseError::Nom(input, kind)
  }

  fn append(_: I, _: ErrorKind, other: Self) -> Self {
    other
  }
}

/// Message value when key is an OffsetKey
#[derive(Debug, Clone)]
pub struct OffsetCommitValue<'a> {
    pub version: u16,
    pub offset: u64,
    pub leader_epoch: Option<u32>,
    metadata: Cow<'a, str>,
    pub commit_timestamp: u64,
    pub expire_timestamp: Option<u64>,
}

impl<'a> OffsetCommitValue<'a> {
    pub fn metadata(&self) -> &str {
        &self.metadata
    }
}

impl<'a> TryFrom<&'a [u8]> for OffsetCommitValue<'a> {
    type Error = ConsumerOffsetsMessageParseError<&'a [u8]>;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        match parse_offset_commit_value(bytes) {
            Ok((_, res)) => Ok(res),
            Err(nom::Err::Error(err)) |
            Err(nom::Err::Failure(err)) => Err(err),
            Err(nom::Err::Incomplete(_)) => Err(ConsumerOffsetsMessageParseError::Incomplete),
        }
    }
}

/// The message key for messages on the __consumer_offsets topic
#[derive(Debug, Clone)]
pub enum ConsumerOffsetsMessageKey<'a> {
    /// Key for OffsetCommitValue message
    Offset(OffsetKey<'a>),

    /// Key for a GroupMetadata message.
    ///
    /// We don't actually parse this key, but since it's a possible variant we
    /// include a tag here so it can be handled properly vs inspecting errors to
    /// check if parsing failed because we haven't implemented this key type.
    GroupMetadata,
}

/// Data for the OffsetKey variant
#[derive(Debug, Clone)]
pub struct OffsetKey<'a> {
    group: Cow<'a, str>,
    topic: Cow<'a, str>,
    pub partition: i32,
}

impl<'a> OffsetKey<'a> {
    pub fn group(&self) -> &str {
        &self.group
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }
}

impl<'a> TryFrom<&'a [u8]> for ConsumerOffsetsMessageKey<'a> {
    type Error = ConsumerOffsetsMessageParseError<&'a [u8]>;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        match parse_consumer_offsets_message_key(bytes) {
            Ok((_, res)) => Ok(res),
            Err(nom::Err::Error(err)) |
            Err(nom::Err::Failure(err)) => Err(err),
            Err(nom::Err::Incomplete(_)) => Err(ConsumerOffsetsMessageParseError::Incomplete),
        }
    }
}

// -------------------------
// nom parsing functions
// -------------------------

/// takes first two bytes as big endian u16 length; then uses that to parse
/// utf-8 string.
fn length_str(bytes: &[u8]) -> IResult<&[u8], &str> {
    let (bytes, sbuf) = length_data(be_u16)(bytes)?;
    match std::str::from_utf8(sbuf) {
        Ok(s) => Ok((bytes, s)),
        Err(e) => Err(nom::Err::Error(From::from(e)))
    }
}

fn unsigned_varint(bytes: &[u8]) -> IResult<&[u8], u64> {
    let mut value = 0_u64;
    let mut shift = 0_u32;
    let mut idx = 0_usize;

    while idx < bytes.len() && idx < 10 {
        let b = bytes[idx];
        value |= ((b & 0x7f) as u64) << shift;
        idx += 1;

        if b & 0x80 == 0 {
            return Ok((&bytes[idx..], value));
        }

        shift += 7;
    }

    Err(nom::Err::Error(ConsumerOffsetsMessageParseError::Nom(bytes, ErrorKind::Fail)))
}

fn compact_str(bytes: &[u8]) -> IResult<&[u8], &str> {
    let (bytes, len_plus_one) = unsigned_varint(bytes)?;
    if len_plus_one == 0 {
        return Err(nom::Err::Error(ConsumerOffsetsMessageParseError::Nom(bytes, ErrorKind::Fail)));
    }

    let len = usize::try_from(len_plus_one - 1).map_err(|_| {
        nom::Err::Error(ConsumerOffsetsMessageParseError::Nom(bytes, ErrorKind::Fail))
    })?;
    let (bytes, sbuf) = take(len)(bytes)?;
    match std::str::from_utf8(sbuf) {
        Ok(s) => Ok((bytes, s)),
        Err(e) => Err(nom::Err::Error(From::from(e))),
    }
}

fn skip_tagged_fields(bytes: &[u8]) -> IResult<&[u8], ()> {
    let (mut bytes, num_tags) = unsigned_varint(bytes)?;
    let num_tags = usize::try_from(num_tags).map_err(|_| {
        nom::Err::Error(ConsumerOffsetsMessageParseError::Nom(bytes, ErrorKind::Fail))
    })?;

    for _ in 0..num_tags {
        let (next, _) = unsigned_varint(bytes)?; // tag id
        let (next, size) = unsigned_varint(next)?;
        let size = usize::try_from(size).map_err(|_| {
            nom::Err::Error(ConsumerOffsetsMessageParseError::Nom(next, ErrorKind::Fail))
        })?;
        let (next, _) = take(size)(next)?;
        bytes = next;
    }

    Ok((bytes, ()))
}

fn parse_offset_commit_value0(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue<'_>> {
    let (bytes, (offset, metadata, commit_timestamp)) = tuple((be_u64, length_str, be_u64))(bytes)?;
    Ok((bytes, OffsetCommitValue {
        version: 0,
        offset,
        metadata: Cow::Borrowed(metadata),
        commit_timestamp,
        expire_timestamp: None,
        leader_epoch: None,
    }))
}

fn parse_offset_commit_value1(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue<'_>> {
    let (bytes, (offset, metadata, commit_timestamp, expire_timestamp)) = tuple((be_u64, length_str, be_u64, be_u64))(bytes)?;
    Ok((bytes, OffsetCommitValue {
        version: 1,
        offset,
        metadata: Cow::Borrowed(metadata),
        commit_timestamp,
        expire_timestamp: Some(expire_timestamp),
        leader_epoch: None,
    }))
}

fn parse_offset_commit_value3(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue<'_>> {
    let (bytes, (offset, leader_epoch, metadata, commit_timestamp)) = tuple((be_u64, be_u32, length_str, be_u64))(bytes)?;
    Ok((bytes, OffsetCommitValue {
        version: 3,
        offset,
        metadata: Cow::Borrowed(metadata),
        commit_timestamp,
        leader_epoch: if leader_epoch == u32::MAX { None } else { Some(leader_epoch) },
        expire_timestamp: None,
    }))
}

fn parse_offset_commit_value4(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue<'_>> {
    let (bytes, offset) = be_u64(bytes)?;
    let (bytes, leader_epoch) = be_i32(bytes)?;
    let (bytes, metadata) = compact_str(bytes)?;
    let (bytes, commit_timestamp) = be_u64(bytes)?;
    let (bytes, ()) = skip_tagged_fields(bytes)?;
    Ok((bytes, OffsetCommitValue {
        version: 4,
        offset,
        metadata: Cow::Borrowed(metadata),
        commit_timestamp,
        leader_epoch: if leader_epoch == -1 { None } else { Some(leader_epoch as u32) },
        expire_timestamp: None,
    }))
}

fn parse_offset_commit_value(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue<'_>> {
    let (bytes, version) = be_u16(bytes)?;
    match version {
        0     => parse_offset_commit_value0(bytes),
        1..=2 => parse_offset_commit_value1(bytes),
        3     => parse_offset_commit_value3(bytes),
        4     => parse_offset_commit_value4(bytes),
        _ => Err(nom::Err::Error(ConsumerOffsetsMessageParseError::Nom(bytes, ErrorKind::Fail)))
    }
}

fn parse_offset_key(bytes: &[u8]) -> IResult<&[u8], ConsumerOffsetsMessageKey<'_>> {
    let (bytes, (group, topic, partition)) = tuple((length_str, length_str, be_i32))(bytes)?;

    let offset_key = OffsetKey {
        group: Cow::Borrowed(group),
        topic: Cow::Borrowed(topic),
        partition
    };
    Ok((bytes, ConsumerOffsetsMessageKey::Offset(offset_key)))
}

fn parse_consumer_offsets_message_key(bytes: &[u8]) -> IResult<&[u8], ConsumerOffsetsMessageKey<'_>> {
    let (bytes, version) = be_u16(bytes)?;
    match version {
        0..=1 => parse_offset_key(bytes),
        _ => Ok((&[], ConsumerOffsetsMessageKey::GroupMetadata))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn assert_clone<T: Clone + ToOwned>() {}

    #[test]
    fn is_cloneable() {
        assert_clone::<OffsetCommitValue>();
        assert_clone::<ConsumerOffsetsMessageKey>();
    }

    #[test]
    fn parse_offset_commit_value_v4_from_production_payload() {
        let bytes: &[u8] = &[
            0, 4, // value version
            0, 0, 0, 2, 8, 80, 63, 149, // offset
            255, 255, 255, 255, // leader epoch (-1 => none)
            1, // compact metadata length+1 => empty string
            0, 0, 1, 156, 72, 66, 222, 171, // commit timestamp
            0, // tagged fields count
        ];

        let value = OffsetCommitValue::try_from(bytes).expect("v4 payload should parse");
        assert_eq!(value.version, 4);
        assert_eq!(value.offset, 8_729_411_477);
        assert_eq!(value.leader_epoch, None);
        assert_eq!(value.metadata(), "");
        assert_eq!(value.commit_timestamp, 1_770_738_867_883);
    }

    #[test]
    fn parse_all_reported_crash_payloads() {
        let payloads: [&[u8]; 4] = [
            &[0, 0, 0, 2, 8, 80, 63, 149, 255, 255, 255, 255, 1, 0, 0, 1, 156, 72, 66, 222, 171, 0],
            &[0, 0, 0, 0, 213, 22, 211, 204, 255, 255, 255, 255, 1, 0, 0, 1, 156, 72, 66, 223, 35, 0],
            &[0, 0, 0, 2, 102, 216, 193, 64, 255, 255, 255, 255, 1, 0, 0, 1, 156, 72, 66, 219, 110, 0],
            &[0, 0, 0, 2, 99, 255, 177, 36, 255, 255, 255, 255, 1, 0, 0, 1, 156, 72, 66, 219, 0, 0],
        ];

        for body in payloads {
            let mut raw = Vec::with_capacity(body.len() + 2);
            raw.push(0);
            raw.push(4);
            raw.extend_from_slice(body);

            let value = OffsetCommitValue::try_from(raw.as_slice()).expect("reported v4 payload should parse");
            assert_eq!(value.version, 4);
            assert_eq!(value.leader_epoch, None);
            assert_eq!(value.metadata(), "");
            assert!(value.commit_timestamp > 0);
        }
    }
}
