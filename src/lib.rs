use std::borrow::Cow;
use std::convert::TryFrom;

use thiserror::Error;

use nom::error::{ErrorKind, ParseError};
use nom::multi::length_data;
use nom::number::complete::{be_i32, be_u16, be_u32, be_u64};
use nom::sequence::tuple;

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
            Err(nom::Err::Error(err)) | Err(nom::Err::Failure(err)) => Err(err),
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
            Err(nom::Err::Error(err)) | Err(nom::Err::Failure(err)) => Err(err),
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
        Err(e) => Err(nom::Err::Error(From::from(e))),
    }
}

/// Parse an unsigned varint (unsigned LEB128 encoding) used by Kafka's
/// flexible versioning protocol.
fn unsigned_varint(bytes: &[u8]) -> IResult<&[u8], u64> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut i = 0;

    loop {
        if i >= bytes.len() {
            return Err(nom::Err::Error(
                ConsumerOffsetsMessageParseError::Incomplete,
            ));
        }
        let byte = bytes[i];
        result |= ((byte & 0x7F) as u64) << shift;
        i += 1;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    Ok((&bytes[i..], result))
}

/// Parse a compact string (used in Kafka flexible versioning).
/// Encoded as unsigned varint N followed by N-1 bytes of UTF-8.
/// N=0 represents null, which is invalid for non-nullable string fields.
fn compact_length_str(bytes: &[u8]) -> IResult<&[u8], &str> {
    let (bytes, len) = unsigned_varint(bytes)?;
    if len == 0 {
        return Err(nom::Err::Error(ConsumerOffsetsMessageParseError::Nom(
            bytes,
            ErrorKind::Verify,
        )));
    }
    let str_len = (len - 1) as usize;
    if bytes.len() < str_len {
        return Err(nom::Err::Error(
            ConsumerOffsetsMessageParseError::Incomplete,
        ));
    }
    let (str_bytes, rest) = bytes.split_at(str_len);
    match std::str::from_utf8(str_bytes) {
        Ok(s) => Ok((rest, s)),
        Err(e) => Err(nom::Err::Error(From::from(e))),
    }
}

/// Skip the tagged field buffer appended to records using flexible versioning.
/// Encoded as unsigned varint count, then for each tag: varint tag id, varint
/// size, followed by that many bytes of data.
fn skip_tag_buffer(bytes: &[u8]) -> IResult<&[u8], ()> {
    let (mut bytes, num_tags) = unsigned_varint(bytes)?;
    for _ in 0..num_tags {
        let (rest, _tag) = unsigned_varint(bytes)?;
        let (rest, size) = unsigned_varint(rest)?;
        let size = size as usize;
        if rest.len() < size {
            return Err(nom::Err::Error(
                ConsumerOffsetsMessageParseError::Incomplete,
            ));
        }
        bytes = &rest[size..];
    }
    Ok((bytes, ()))
}

fn parse_offset_commit_value0(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue<'_>> {
    let (bytes, (offset, metadata, commit_timestamp)) = tuple((be_u64, length_str, be_u64))(bytes)?;
    Ok((
        bytes,
        OffsetCommitValue {
            version: 0,
            offset,
            metadata: Cow::Borrowed(metadata),
            commit_timestamp,
            expire_timestamp: None,
            leader_epoch: None,
        },
    ))
}

fn parse_offset_commit_value1(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue<'_>> {
    let (bytes, (offset, metadata, commit_timestamp, expire_timestamp)) =
        tuple((be_u64, length_str, be_u64, be_u64))(bytes)?;
    Ok((
        bytes,
        OffsetCommitValue {
            version: 1,
            offset,
            metadata: Cow::Borrowed(metadata),
            commit_timestamp,
            expire_timestamp: Some(expire_timestamp),
            leader_epoch: None,
        },
    ))
}

fn parse_offset_commit_value3(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue<'_>> {
    let (bytes, (offset, leader_epoch, metadata, commit_timestamp)) =
        tuple((be_u64, be_u32, length_str, be_u64))(bytes)?;
    Ok((
        bytes,
        OffsetCommitValue {
            version: 3,
            offset,
            metadata: Cow::Borrowed(metadata),
            commit_timestamp,
            leader_epoch: if leader_epoch == u32::MAX {
                None
            } else {
                Some(leader_epoch)
            },
            expire_timestamp: None,
        },
    ))
}

/// Version 4: flexible versioning. Same fixed fields as v3 but uses compact
/// strings and has a trailing tagged field buffer (which may contain a topicId
/// UUID at tag 0). We parse the fixed fields and skip the tag buffer.
fn parse_offset_commit_value4(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue<'_>> {
    let (bytes, offset) = be_u64(bytes)?;
    let (bytes, leader_epoch) = be_u32(bytes)?;
    let (bytes, metadata) = compact_length_str(bytes)?;
    let (bytes, commit_timestamp) = be_u64(bytes)?;
    let (bytes, ()) = skip_tag_buffer(bytes)?;
    Ok((
        bytes,
        OffsetCommitValue {
            version: 4,
            offset,
            metadata: Cow::Borrowed(metadata),
            commit_timestamp,
            leader_epoch: if leader_epoch == u32::MAX {
                None
            } else {
                Some(leader_epoch)
            },
            expire_timestamp: None,
        },
    ))
}

fn parse_offset_commit_value(bytes: &[u8]) -> IResult<&[u8], OffsetCommitValue<'_>> {
    let (bytes, version) = be_u16(bytes)?;
    match version {
        0 => parse_offset_commit_value0(bytes),
        1..=2 => parse_offset_commit_value1(bytes),
        3 => parse_offset_commit_value3(bytes),
        4 => parse_offset_commit_value4(bytes),
        _ => Err(nom::Err::Error(ConsumerOffsetsMessageParseError::Nom(
            bytes,
            ErrorKind::Fail,
        ))),
    }
}

fn parse_offset_key(bytes: &[u8]) -> IResult<&[u8], ConsumerOffsetsMessageKey<'_>> {
    let (bytes, (group, topic, partition)) = tuple((length_str, length_str, be_i32))(bytes)?;

    let offset_key = OffsetKey {
        group: Cow::Borrowed(group),
        topic: Cow::Borrowed(topic),
        partition,
    };
    Ok((bytes, ConsumerOffsetsMessageKey::Offset(offset_key)))
}

fn parse_consumer_offsets_message_key(
    bytes: &[u8],
) -> IResult<&[u8], ConsumerOffsetsMessageKey<'_>> {
    let (bytes, version) = be_u16(bytes)?;
    match version {
        0..=1 => parse_offset_key(bytes),
        _ => Ok((&[], ConsumerOffsetsMessageKey::GroupMetadata)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryFrom;

    fn assert_clone<T: Clone + ToOwned>() {}

    #[test]
    fn is_cloneable() {
        assert_clone::<OffsetCommitValue>();
        assert_clone::<ConsumerOffsetsMessageKey>();
    }

    #[test]
    fn parse_unsigned_varint_single_byte() {
        let (rest, val) = unsigned_varint(&[0x05, 0xFF]).unwrap();
        assert_eq!(val, 5);
        assert_eq!(rest, &[0xFF]);
    }

    #[test]
    fn parse_unsigned_varint_multi_byte() {
        // 300 = 0b100101100 -> [0xAC, 0x02]
        let (rest, val) = unsigned_varint(&[0xAC, 0x02]).unwrap();
        assert_eq!(val, 300);
        assert_eq!(rest, &[]);
    }

    #[test]
    fn parse_compact_string_empty() {
        // varint 1 means length 0 (empty string)
        let (rest, s) = compact_length_str(&[0x01, 0xAA]).unwrap();
        assert_eq!(s, "");
        assert_eq!(rest, &[0xAA]);
    }

    #[test]
    fn parse_compact_string_hello() {
        // "hello" has length 5, compact encoding = varint(6) = 0x06
        let mut buf = vec![0x06];
        buf.extend_from_slice(b"hello");
        buf.push(0xFF); // trailing byte
        let (rest, s) = compact_length_str(&buf).unwrap();
        assert_eq!(s, "hello");
        assert_eq!(rest, &[0xFF]);
    }

    #[test]
    fn parse_compact_string_null_is_error() {
        // varint 0 = null, should fail for non-nullable
        assert!(compact_length_str(&[0x00]).is_err());
    }

    #[test]
    fn skip_empty_tag_buffer() {
        // 0 tags
        let (rest, ()) = skip_tag_buffer(&[0x00, 0xBB]).unwrap();
        assert_eq!(rest, &[0xBB]);
    }

    #[test]
    fn skip_tag_buffer_with_one_tag() {
        // 1 tag, tag_id=0, size=2, data=[0xAA, 0xBB]
        let buf = &[0x01, 0x00, 0x02, 0xAA, 0xBB, 0xCC];
        let (rest, ()) = skip_tag_buffer(buf).unwrap();
        assert_eq!(rest, &[0xCC]);
    }

    /// Build a v4 binary payload for testing.
    fn build_v4_bytes(
        offset: u64,
        leader_epoch: u32,
        metadata: &str,
        commit_timestamp: u64,
        tag_data: &[u8],
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        // version
        buf.extend_from_slice(&4u16.to_be_bytes());
        // offset
        buf.extend_from_slice(&offset.to_be_bytes());
        // leader_epoch
        buf.extend_from_slice(&leader_epoch.to_be_bytes());
        // compact string: varint(len+1) then bytes
        let str_len = (metadata.len() + 1) as u8;
        buf.push(str_len);
        buf.extend_from_slice(metadata.as_bytes());
        // commit_timestamp
        buf.extend_from_slice(&commit_timestamp.to_be_bytes());
        // tag buffer
        buf.extend_from_slice(tag_data);
        buf
    }

    #[test]
    fn parse_v4_no_tags() {
        let buf = build_v4_bytes(42, 5, "", 1000, &[0x00]);
        let val = OffsetCommitValue::try_from(buf.as_slice()).unwrap();
        assert_eq!(val.version, 4);
        assert_eq!(val.offset, 42);
        assert_eq!(val.leader_epoch, Some(5));
        assert_eq!(val.metadata(), "");
        assert_eq!(val.commit_timestamp, 1000);
        assert_eq!(val.expire_timestamp, None);
    }

    #[test]
    fn parse_v4_with_metadata() {
        let buf = build_v4_bytes(100, u32::MAX, "some-meta", 2000, &[0x00]);
        let val = OffsetCommitValue::try_from(buf.as_slice()).unwrap();
        assert_eq!(val.version, 4);
        assert_eq!(val.offset, 100);
        assert_eq!(val.leader_epoch, None); // u32::MAX => None
        assert_eq!(val.metadata(), "some-meta");
        assert_eq!(val.commit_timestamp, 2000);
    }

    #[test]
    fn parse_v4_with_topic_id_tag() {
        // tag buffer: 1 tag, tag_id=0, size=16, then 16 bytes of UUID
        let mut tag_buf = vec![0x01, 0x00, 0x10];
        tag_buf.extend_from_slice(&[
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
            0x0F, 0x10,
        ]);
        let buf = build_v4_bytes(50, 3, "m", 3000, &tag_buf);
        let val = OffsetCommitValue::try_from(buf.as_slice()).unwrap();
        assert_eq!(val.version, 4);
        assert_eq!(val.offset, 50);
        assert_eq!(val.leader_epoch, Some(3));
        assert_eq!(val.metadata(), "m");
        assert_eq!(val.commit_timestamp, 3000);
    }

    #[test]
    fn parse_v3_still_works() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&3u16.to_be_bytes()); // version
        buf.extend_from_slice(&99u64.to_be_bytes()); // offset
        buf.extend_from_slice(&7u32.to_be_bytes()); // leader_epoch
        buf.extend_from_slice(&0u16.to_be_bytes()); // metadata length (u16 for v3)
        buf.extend_from_slice(&5000u64.to_be_bytes()); // commit_timestamp
        let val = OffsetCommitValue::try_from(buf.as_slice()).unwrap();
        assert_eq!(val.version, 3);
        assert_eq!(val.offset, 99);
        assert_eq!(val.leader_epoch, Some(7));
        assert_eq!(val.commit_timestamp, 5000);
    }

    #[test]
    fn parse_v0_still_works() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u16.to_be_bytes()); // version
        buf.extend_from_slice(&10u64.to_be_bytes()); // offset
        buf.extend_from_slice(&0u16.to_be_bytes()); // metadata length
        buf.extend_from_slice(&1234u64.to_be_bytes()); // commit_timestamp
        let val = OffsetCommitValue::try_from(buf.as_slice()).unwrap();
        assert_eq!(val.version, 0);
        assert_eq!(val.offset, 10);
        assert_eq!(val.leader_epoch, None);
        assert_eq!(val.expire_timestamp, None);
        assert_eq!(val.commit_timestamp, 1234);
    }

    #[test]
    fn parse_v5_is_error() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&5u16.to_be_bytes());
        buf.extend_from_slice(&[0u8; 30]);
        assert!(OffsetCommitValue::try_from(buf.as_slice()).is_err());
    }
}
