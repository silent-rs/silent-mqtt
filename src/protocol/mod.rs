use std::convert::{Infallible, TryFrom};
use std::pin::Pin;
use std::str;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use silent::Protocol;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    pub topic_filter: String,
    pub qos: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribePacket {
    pub packet_identifier: u16,
    pub subscriptions: Vec<Subscription>,
}

impl SubscribePacket {
    pub fn decode(src: &[u8]) -> Result<(Self, usize), ProtocolError> {
        if src.len() < 2 {
            return Err(ProtocolError::UnexpectedEof);
        }
        let control_byte = src[0];
        if control_byte >> 4 != 0x08 {
            return Err(ProtocolError::InvalidPacketType(control_byte >> 4));
        }
        if control_byte & 0x0F != 0x02 {
            return Err(ProtocolError::InvalidSubscribeFlags(control_byte & 0x0F));
        }

        let (remaining_length, remaining_len_bytes) = decode_remaining_length(&src[1..])?;
        let header_len = 1 + remaining_len_bytes;
        let total_len = header_len + remaining_length;
        if src.len() < total_len {
            return Err(ProtocolError::RemainingLengthMismatch {
                expected: remaining_length,
                actual: src.len().saturating_sub(header_len),
            });
        }

        let mut cursor = header_len;
        let end = header_len + remaining_length;
        let packet_identifier = read_u16(src, &mut cursor, end)?;
        let mut subscriptions = Vec::new();

        while cursor < end {
            let topic_filter = read_utf8_string(src, &mut cursor, end)?.to_string();
            if topic_filter.is_empty() {
                return Err(ProtocolError::EmptyTopicFilter);
            }
            let qos = read_u8(src, &mut cursor, end)?;
            if qos > 1 {
                return Err(ProtocolError::UnsupportedQoS(qos));
            }
            subscriptions.push(Subscription { topic_filter, qos });
        }

        if subscriptions.is_empty() {
            return Err(ProtocolError::EmptySubscriptions);
        }

        Ok((
            SubscribePacket {
                packet_identifier,
                subscriptions,
            },
            total_len,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SubscribeReturnCode {
    SuccessQoS0 = 0x00,
    SuccessQoS1 = 0x01,
    Failure = 0x80,
}

impl From<SubscribeReturnCode> for u8 {
    fn from(value: SubscribeReturnCode) -> Self {
        value as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAckPacket {
    pub packet_identifier: u16,
    pub return_codes: Vec<SubscribeReturnCode>,
}

impl SubAckPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        if self.return_codes.is_empty() {
            return Err(ProtocolError::EmptySubscriptions);
        }
        let mut variable = Vec::with_capacity(2 + self.return_codes.len());
        variable.extend_from_slice(&self.packet_identifier.to_be_bytes());
        for code in &self.return_codes {
            variable.push((*code).into());
        }

        let mut buf = Vec::with_capacity(1 + 4 + variable.len());
        buf.push(0x90);
        encode_remaining_length(&mut buf, variable.len())?;
        buf.extend_from_slice(&variable);
        Ok(buf)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishPacket {
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
    pub topic_name: String,
    pub packet_identifier: Option<u16>,
    pub payload: Bytes,
}

impl PublishPacket {
    pub fn decode(src: &[u8]) -> Result<(Self, usize), ProtocolError> {
        if src.len() < 2 {
            return Err(ProtocolError::UnexpectedEof);
        }
        let control_byte = src[0];
        if control_byte >> 4 != 0x03 {
            return Err(ProtocolError::InvalidPacketType(control_byte >> 4));
        }

        let dup = (control_byte & 0x08) != 0;
        let qos = (control_byte & 0x06) >> 1;
        let retain = (control_byte & 0x01) != 0;
        if qos > 1 {
            return Err(ProtocolError::UnsupportedQoS(qos));
        }
        if qos == 0 && dup {
            return Err(ProtocolError::InvalidPublishFlags(control_byte & 0x0F));
        }

        let (remaining_length, remaining_len_bytes) = decode_remaining_length(&src[1..])?;
        let header_len = 1 + remaining_len_bytes;
        let total_len = header_len + remaining_length;
        if src.len() < total_len {
            return Err(ProtocolError::RemainingLengthMismatch {
                expected: remaining_length,
                actual: src.len().saturating_sub(header_len),
            });
        }

        let mut cursor = header_len;
        let end = header_len + remaining_length;
        let topic_name = read_utf8_string(src, &mut cursor, end)?.to_string();

        let packet_identifier = if qos > 0 {
            Some(read_u16(src, &mut cursor, end)?)
        } else {
            None
        };

        if qos == 0 && packet_identifier.is_some() {
            return Err(ProtocolError::UnexpectedPacketIdentifier);
        }
        if qos > 0 && packet_identifier.is_none() {
            return Err(ProtocolError::MissingPacketIdentifier);
        }

        let payload = Bytes::copy_from_slice(&src[cursor..end]);

        Ok((
            PublishPacket {
                dup,
                qos,
                retain,
                topic_name,
                packet_identifier,
                payload,
            },
            total_len,
        ))
    }

    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        if self.qos > 1 {
            return Err(ProtocolError::UnsupportedQoS(self.qos));
        }
        if self.qos == 0 && self.packet_identifier.is_some() {
            return Err(ProtocolError::UnexpectedPacketIdentifier);
        }
        if self.qos > 0 && self.packet_identifier.is_none() {
            return Err(ProtocolError::MissingPacketIdentifier);
        }

        let mut variable = Vec::new();
        write_utf8_string(&mut variable, &self.topic_name)?;
        if let Some(packet_id) = self.packet_identifier {
            variable.extend_from_slice(&packet_id.to_be_bytes());
        }
        variable.extend_from_slice(&self.payload);

        let mut buf = Vec::with_capacity(1 + 4 + variable.len());
        let mut fixed_header = 0x30;
        if self.dup {
            fixed_header |= 0x08;
        }
        fixed_header |= (self.qos & 0x03) << 1;
        if self.retain {
            fixed_header |= 0x01;
        }
        buf.push(fixed_header);
        encode_remaining_length(&mut buf, variable.len())?;
        buf.extend_from_slice(&variable);
        Ok(buf)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PubAckPacket {
    pub packet_identifier: u16,
}

impl PubAckPacket {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut buf = Vec::with_capacity(4);
        buf.push(0x40);
        buf.push(0x02);
        buf.extend_from_slice(&self.packet_identifier.to_be_bytes());
        Ok(buf)
    }
}

/// 最大可编码的 MQTT 剩余长度（4 字节可变长度编码上限）。
const MAX_REMAINING_LENGTH: usize = 268_435_455;

/// CONNECT 报文结构（MQTT 3.1.1）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectPacket {
    pub client_id: String,
    pub clean_session: bool,
    pub keep_alive: u16,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
}

impl ConnectPacket {
    /// 从原始字节流解析 CONNECT 报文。
    pub fn decode(src: &[u8]) -> Result<(Self, usize), ProtocolError> {
        if src.len() < 2 {
            return Err(ProtocolError::UnexpectedEof);
        }

        let control_byte = src[0];
        let packet_type = control_byte >> 4;
        let flags = control_byte & 0x0F;
        if packet_type != 0x01 {
            return Err(ProtocolError::InvalidPacketType(packet_type));
        }
        if flags != 0 {
            return Err(ProtocolError::InvalidPacketFlags(flags));
        }

        let (remaining_length, remaining_len_bytes) = decode_remaining_length(&src[1..])?;
        let header_len = 1 + remaining_len_bytes;
        let total_len = header_len + remaining_length;

        if remaining_length > MAX_REMAINING_LENGTH {
            return Err(ProtocolError::RemainingLengthTooLarge);
        }
        if src.len() < total_len {
            return Err(ProtocolError::RemainingLengthMismatch {
                expected: remaining_length,
                actual: src.len().saturating_sub(header_len),
            });
        }

        let mut cursor = header_len;
        let end = header_len + remaining_length;

        let protocol_name = read_utf8_string(src, &mut cursor, end)?;
        if protocol_name != "MQTT" {
            return Err(ProtocolError::UnsupportedProtocolName(
                protocol_name.to_string(),
            ));
        }

        let protocol_level = read_u8(src, &mut cursor, end)?;
        if protocol_level != 0x04 {
            return Err(ProtocolError::UnsupportedProtocolLevel(protocol_level));
        }

        let connect_flags = read_u8(src, &mut cursor, end)?;
        if connect_flags & 0x01 != 0 {
            return Err(ProtocolError::InvalidConnectFlags);
        }
        let clean_session = (connect_flags & 0x02) != 0;
        let will_flag = (connect_flags & 0x04) != 0;
        if will_flag {
            return Err(ProtocolError::WillFlagNotSupported);
        }
        let password_flag = (connect_flags & 0x40) != 0;
        let username_flag = (connect_flags & 0x80) != 0;
        if password_flag && !username_flag {
            return Err(ProtocolError::PasswordFlagRequiresUsername);
        }

        let keep_alive = read_u16(src, &mut cursor, end)?;
        let client_id = read_utf8_string(src, &mut cursor, end)?.to_string();

        let username = if username_flag {
            Some(read_utf8_string(src, &mut cursor, end)?.to_string())
        } else {
            None
        };

        let password = if password_flag {
            Some(read_binary_data(src, &mut cursor, end)?)
        } else {
            None
        };

        Ok((
            ConnectPacket {
                client_id,
                clean_session,
                keep_alive,
                username,
                password,
            },
            total_len,
        ))
    }

    /// 将 CONNECT 报文编码为字节流。
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let mut variable = Vec::new();
        write_utf8_string(&mut variable, "MQTT")?;
        variable.push(0x04); // Protocol Level

        let mut flags = 0u8;
        if self.clean_session {
            flags |= 0x02;
        }
        if self.username.is_some() {
            flags |= 0x80;
        }
        if self.password.is_some() {
            flags |= 0x40;
            if self.username.is_none() {
                return Err(ProtocolError::PasswordFlagRequiresUsername);
            }
        }
        variable.push(flags);
        variable.extend_from_slice(&self.keep_alive.to_be_bytes());
        write_utf8_string(&mut variable, &self.client_id)?;
        if let Some(username) = &self.username {
            write_utf8_string(&mut variable, username)?;
        }
        if let Some(password) = &self.password {
            write_binary_data(&mut variable, password)?;
        }

        let remaining_length = variable.len();
        let mut buf = Vec::with_capacity(1 + 4 + remaining_length);
        buf.push(0x10);
        encode_remaining_length(&mut buf, remaining_length)?;
        buf.extend_from_slice(&variable);
        Ok(buf)
    }
}

/// CONNACK 返回码。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ConnectReturnCode {
    Accepted = 0x00,
    UnacceptableProtocolVersion = 0x01,
    IdentifierRejected = 0x02,
    ServerUnavailable = 0x03,
    BadUsernameOrPassword = 0x04,
    NotAuthorized = 0x05,
}

impl TryFrom<u8> for ConnectReturnCode {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Accepted),
            0x01 => Ok(Self::UnacceptableProtocolVersion),
            0x02 => Ok(Self::IdentifierRejected),
            0x03 => Ok(Self::ServerUnavailable),
            0x04 => Ok(Self::BadUsernameOrPassword),
            0x05 => Ok(Self::NotAuthorized),
            _ => Err(ProtocolError::InvalidReturnCode(value)),
        }
    }
}

impl From<ConnectReturnCode> for u8 {
    fn from(value: ConnectReturnCode) -> Self {
        value as u8
    }
}

/// CONNACK 报文结构。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnAckPacket {
    pub session_present: bool,
    pub return_code: ConnectReturnCode,
}

impl ConnAckPacket {
    /// 编码为字节流。
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4);
        buf.push(0x20);
        buf.push(0x02);
        buf.push(if self.session_present { 0x01 } else { 0x00 });
        buf.push(self.return_code.into());
        buf
    }

    /// 从字节流解析 CONNACK 报文。
    pub fn decode(src: &[u8]) -> Result<(Self, usize), ProtocolError> {
        if src.len() < 4 {
            return Err(ProtocolError::UnexpectedEof);
        }
        let control_byte = src[0];
        let packet_type = control_byte >> 4;
        let flags = control_byte & 0x0F;
        if packet_type != 0x02 {
            return Err(ProtocolError::InvalidPacketType(packet_type));
        }
        if flags != 0 {
            return Err(ProtocolError::InvalidPacketFlags(flags));
        }

        let (remaining_length, remaining_len_bytes) = decode_remaining_length(&src[1..])?;
        if remaining_length != 2 {
            return Err(ProtocolError::InvalidConnAckLength(remaining_length));
        }

        let header_len = 1 + remaining_len_bytes;
        let total_len = header_len + remaining_length;
        if src.len() < total_len {
            return Err(ProtocolError::RemainingLengthMismatch {
                expected: remaining_length,
                actual: src.len().saturating_sub(header_len),
            });
        }

        let mut cursor = header_len;
        let end = header_len + remaining_length;
        let ack_flags = read_u8(src, &mut cursor, end)?;
        if ack_flags & !0x01 != 0 {
            return Err(ProtocolError::InvalidConnAckFlags(ack_flags));
        }
        let session_present = (ack_flags & 0x01) != 0;
        let return_code = ConnectReturnCode::try_from(read_u8(src, &mut cursor, end)?)?;

        Ok((
            ConnAckPacket {
                session_present,
                return_code,
            },
            total_len,
        ))
    }
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid packet type: {0:#x}")]
    InvalidPacketType(u8),
    #[error("invalid packet flags: {0:#x}")]
    InvalidPacketFlags(u8),
    #[error("invalid SUBSCRIBE flags: {0:#x}")]
    InvalidSubscribeFlags(u8),
    #[error("SUBSCRIBE payload must contain at least one topic filter")]
    EmptySubscriptions,
    #[error("topic filter must not be empty")]
    EmptyTopicFilter,
    #[error("requested QoS not supported: {0}")]
    UnsupportedQoS(u8),
    #[error("PUBLISH flags invalid: {0:#x}")]
    InvalidPublishFlags(u8),
    #[error("packet identifier required for QoS > 0")]
    MissingPacketIdentifier,
    #[error("packet identifier must be absent when QoS == 0")]
    UnexpectedPacketIdentifier,
    #[error("malformed remaining length")]
    MalformedRemainingLength,
    #[error("remaining length mismatch (expected {expected}, actual {actual})")]
    RemainingLengthMismatch { expected: usize, actual: usize },
    #[error("unexpected end of packet")]
    UnexpectedEof,
    #[error("unsupported protocol name: {0}")]
    UnsupportedProtocolName(String),
    #[error("unsupported protocol level: {0}")]
    UnsupportedProtocolLevel(u8),
    #[error("reserved connect flag must be zero")]
    InvalidConnectFlags,
    #[error("will message is not supported yet")]
    WillFlagNotSupported,
    #[error("password flag requires username flag")]
    PasswordFlagRequiresUsername,
    #[error("string field contains invalid UTF-8")]
    InvalidUtf8(#[from] str::Utf8Error),
    #[error("encoded string exceeds u16::MAX bytes")]
    StringTooLong,
    #[error("remaining length value is too large")]
    RemainingLengthTooLarge,
    #[error("invalid CONNACK remaining length: {0}")]
    InvalidConnAckLength(usize),
    #[error("invalid CONNACK flags: {0:#x}")]
    InvalidConnAckFlags(u8),
    #[error("invalid CONNACK return code: {0:#x}")]
    InvalidReturnCode(u8),
}

fn decode_remaining_length(src: &[u8]) -> Result<(usize, usize), ProtocolError> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    let mut consumed = 0usize;

    for byte in src.iter().copied().take(4) {
        consumed += 1;
        let encoded = byte as usize;
        value += (encoded & 0x7F) * multiplier;
        if multiplier > MAX_REMAINING_LENGTH / 128 {
            return Err(ProtocolError::RemainingLengthTooLarge);
        }
        if (encoded & 0x80) == 0 {
            return Ok((value, consumed));
        }
        multiplier *= 128;
    }

    Err(ProtocolError::MalformedRemainingLength)
}

fn encode_remaining_length(dst: &mut Vec<u8>, mut value: usize) -> Result<(), ProtocolError> {
    if value > MAX_REMAINING_LENGTH {
        return Err(ProtocolError::RemainingLengthTooLarge);
    }

    loop {
        let mut encoded_byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            encoded_byte |= 0x80;
        }
        dst.push(encoded_byte);
        if value == 0 {
            break;
        }
    }

    Ok(())
}

fn read_u8(src: &[u8], cursor: &mut usize, end: usize) -> Result<u8, ProtocolError> {
    if *cursor >= end {
        return Err(ProtocolError::UnexpectedEof);
    }
    let value = src[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_u16(src: &[u8], cursor: &mut usize, end: usize) -> Result<u16, ProtocolError> {
    if *cursor + 2 > end {
        return Err(ProtocolError::UnexpectedEof);
    }
    let value = u16::from_be_bytes([src[*cursor], src[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

fn read_utf8_string<'a>(
    src: &'a [u8],
    cursor: &mut usize,
    end: usize,
) -> Result<&'a str, ProtocolError> {
    let len = read_u16(src, cursor, end)? as usize;
    if *cursor + len > end {
        return Err(ProtocolError::UnexpectedEof);
    }
    let slice = &src[*cursor..*cursor + len];
    *cursor += len;
    Ok(str::from_utf8(slice)?)
}

fn read_binary_data(src: &[u8], cursor: &mut usize, end: usize) -> Result<Vec<u8>, ProtocolError> {
    let len = read_u16(src, cursor, end)? as usize;
    if *cursor + len > end {
        return Err(ProtocolError::UnexpectedEof);
    }
    let data = src[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(data)
}

fn write_utf8_string(dst: &mut Vec<u8>, value: &str) -> Result<(), ProtocolError> {
    let bytes = value.as_bytes();
    if bytes.len() > u16::MAX as usize {
        return Err(ProtocolError::StringTooLong);
    }
    dst.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    dst.extend_from_slice(bytes);
    Ok(())
}

fn write_binary_data(dst: &mut Vec<u8>, value: &[u8]) -> Result<(), ProtocolError> {
    if value.len() > u16::MAX as usize {
        return Err(ProtocolError::StringTooLong);
    }
    dst.extend_from_slice(&(value.len() as u16).to_be_bytes());
    dst.extend_from_slice(value);
    Ok(())
}

/// 框架内部使用的 MQTT 消息枚举。
pub enum MqttMessage {
    Connect(ConnectPacket),
    Subscribe(SubscribePacket),
    Publish(PublishPacket),
    PingReq,
    Disconnect,
}

/// 框架内部使用的 MQTT 响应枚举。
pub enum MqttResponse {
    ConnAck(ConnAckPacket),
    SubAck(SubAckPacket),
    PubAck(PubAckPacket),
    PingResp,
}

/// 用于 MQTT 协议适配的空响应体实现。
pub struct NoopBody;

impl Body for NoopBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(None)
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(0)
    }

    fn is_end_stream(&self) -> bool {
        true
    }
}

/// 基于 Silent `Protocol` trait 的 MQTT 适配器。
pub struct MqttProtocol;

impl Protocol for MqttProtocol {
    type Incoming = Vec<u8>;
    type Outgoing = Result<Option<Vec<u8>>, ProtocolError>;
    type Body = NoopBody;
    type InternalRequest = Result<MqttMessage, ProtocolError>;
    type InternalResponse = MqttResponse;

    fn into_internal(message: Self::Incoming) -> Self::InternalRequest {
        if message.is_empty() {
            return Err(ProtocolError::UnexpectedEof);
        }
        let packet_type = message[0] >> 4;
        match packet_type {
            0x01 => {
                let (packet, _) = ConnectPacket::decode(&message)?;
                Ok(MqttMessage::Connect(packet))
            }
            0x08 => {
                let (packet, _) = SubscribePacket::decode(&message)?;
                Ok(MqttMessage::Subscribe(packet))
            }
            0x03 => {
                let (packet, _) = PublishPacket::decode(&message)?;
                Ok(MqttMessage::Publish(packet))
            }
            0x0C => Ok(MqttMessage::PingReq),
            0x0E => Ok(MqttMessage::Disconnect),
            _ => Err(ProtocolError::InvalidPacketType(packet_type)),
        }
    }

    fn from_internal(response: Self::InternalResponse) -> Self::Outgoing {
        match response {
            MqttResponse::ConnAck(packet) => Ok(Some(packet.encode())),
            MqttResponse::SubAck(packet) => packet.encode().map(Some),
            MqttResponse::PubAck(packet) => packet.encode().map(Some),
            MqttResponse::PingResp => Ok(Some(vec![0xD0, 0x00])),
        }
    }
}
