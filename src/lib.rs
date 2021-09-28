use std::borrow::Cow;
use std::convert::TryFrom;

use nom::number::complete::{be_u16, be_i32, be_u64, be_u32};
use nom::multi::length_data;
use nom::sequence::tuple;
use nom::error::{ErrorKind, ParseError};
use nom::Err::Error;

pub type IResult<I, O> = nom::IResult<I, O, ConsurmeOffsetsMessageParseError<I>>;

/// Error type for our parsers
///
/// Nom doesn't have a Utf8 error built in so we add our own. Weird!
#[derive(Debug)]
pub enum ConsurmeOffsetsMessageParseError<I> {
    FromUtf8Error(std::str::Utf8Error),
    Nom(I, ErrorKind)
}

impl<I> ParseError<I> for ConsurmeOffsetsMessageParseError<I> {
  fn from_error_kind(input: I, kind: ErrorKind) -> Self {
    ConsurmeOffsetsMessageParseError::Nom(input, kind)
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
    type Error = ConsurmeOffsetsMessageParseError<&'a [u8]>;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        match parse_offset_commit_value(bytes) {
            Ok((_, res)) => Ok(res),
            Err(Error(err)) => Err(err),
            _ => unreachable!(),
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
    pub group: Cow<'a, str>,
    pub topic: Cow<'a, str>,
    partition: i32,
}

impl<'a> OffsetKey<'a> {
    pub fn group(&self) -> &str {
        &self.group
    }

    pub fn topic(&self) -> &str {
        &self.group
    }
}

impl<'a> TryFrom<&'a [u8]> for ConsumerOffsetsMessageKey<'a> {
    type Error = ConsurmeOffsetsMessageParseError<&'a [u8]>;

    fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        match parse_consumer_offsets_message_key(bytes) {
            Ok((_, res)) => Ok(res),
            Err(Error(err)) => Err(err),
            _ => unreachable!(),
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
        Err(e) => Err(Error(ConsurmeOffsetsMessageParseError::FromUtf8Error(e)))
    }
}

fn parse_offset_commit_value0(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue> {
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

fn parse_offset_commit_value1(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue> {
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

fn parse_offset_commit_value3(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue> {
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

fn parse_offset_commit_value(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue> {
    let (bytes, version) = be_u16(bytes)?;
    match version {
        0     => parse_offset_commit_value0(bytes),
        1..=2 => parse_offset_commit_value1(bytes),
        3     => parse_offset_commit_value3(bytes),
        _ => Err(Error(ConsurmeOffsetsMessageParseError::Nom(bytes, ErrorKind::Fail)))
    }
}

fn parse_offset_key(bytes: &[u8]) -> IResult<&[u8], ConsumerOffsetsMessageKey> {
    let (bytes, (group, topic, partition)) = tuple((length_str, length_str, be_i32))(bytes)?;

    let offset_key = OffsetKey {
        group: Cow::Borrowed(group),
        topic: Cow::Borrowed(topic),
        partition
    };
    Ok((bytes, ConsumerOffsetsMessageKey::Offset(offset_key)))
}

fn parse_consumer_offsets_message_key(bytes: &[u8]) -> IResult<&[u8], ConsumerOffsetsMessageKey> {
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
}
