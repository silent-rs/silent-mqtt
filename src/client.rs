use std::collections::{HashMap, hash_map::Entry};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use chrono::Local;
use scru128::scru128_string;
use silent::BoxedConnection;
use silent::Protocol;
use thiserror::Error;
use tokio::io::split;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::{Mutex, mpsc};

use crate::broker::Broker;
use crate::protocol::{
    ConnectPacket, LastWill, MqttMessage, MqttProtocol, MqttResponse, ProtocolError, PubAckPacket,
    PublishPacket, SubAckPacket, SubscribePacket,
};

pub type ClientStream = BoxedConnection;

pub struct ClientSession {
    id: String,
    peer: SocketAddr,
    reader: ReadHalf<ClientStream>,
    outbound: mpsc::UnboundedSender<ClientCommand>,
    broker: Arc<Broker>,
    will: Option<LastWill>,
    outbound_qos2: Arc<Mutex<HashMap<u16, OutboundQoS2State>>>,
    inbound_qos2: HashMap<u16, PublishPacket>,
}

pub enum ClientCommand {
    SendPacket(Vec<u8>),
    Publish {
        topic: String,
        payload: Bytes,
        qos: u8,
        retain: bool,
    },
}

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("client output channel closed")]
    ChannelClosed,
    #[error("writer task failed: {0}")]
    WriterTask(#[from] tokio::task::JoinError),
}

impl ClientSession {
    pub async fn start(
        stream: ClientStream,
        peer: SocketAddr,
        connect: ConnectPacket,
        broker: Arc<Broker>,
    ) -> Result<(), ClientError> {
        let client_id = if connect.client_id.is_empty() {
            scru128_string()
        } else {
            connect.client_id.clone()
        };

        let (reader, writer) = split(stream);
        let (tx, rx) = mpsc::unbounded_channel::<ClientCommand>();

        broker.register_client(client_id.clone(), tx.clone()).await;

        let outbound_qos2 = Arc::new(Mutex::new(HashMap::new()));
        let writer_handle = tokio::spawn(write_loop(writer, rx, outbound_qos2.clone()));

        let mut session = ClientSession {
            id: client_id.clone(),
            peer,
            reader,
            outbound: tx.clone(),
            broker: broker.clone(),
            will: connect.will.clone(),
            outbound_qos2,
            inbound_qos2: HashMap::new(),
        };

        println!(
            "[{}] client {} connected from {} (clean_session={}, keep_alive={})",
            Local::now().naive_local(),
            session.id,
            session.peer,
            connect.clean_session,
            connect.keep_alive
        );

        let keep_alive = connect.keep_alive;
        let read_result = session.read_loop(keep_alive).await;

        let (client_id, broker, outbound, will) = session.into_parts();
        broker.unregister_client(&client_id).await;
        drop(outbound);

        let writer_result = match writer_handle.await? {
            Ok(()) => Ok(()),
            Err(err) => Err(ClientError::Io(err)),
        };

        let mut deliver_will =
            matches!(read_result, Ok(SessionEnd::Abnormal)) || read_result.is_err();
        if writer_result.is_err() {
            deliver_will = true;
        }

        if deliver_will && let Some(ref last_will) = will {
            broker.publish_will(last_will).await;
        }

        writer_result?;

        match read_result {
            Ok(_) => Ok(()),
            Err(err) => Err(err),
        }
    }

    async fn read_loop(&mut self, keep_alive: u16) -> Result<SessionEnd, ClientError> {
        let timeout_duration = if keep_alive == 0 {
            None
        } else {
            let secs = (keep_alive as u64).saturating_mul(3).div_ceil(2);
            Some(Duration::from_secs(secs.max(1)))
        };
        loop {
            let packet = if let Some(duration) = timeout_duration {
                match tokio::time::timeout(duration, read_packet(&mut self.reader)).await {
                    Ok(result) => result?,
                    Err(_) => {
                        println!(
                            "[{}] client {} keep-alive timeout ({}s)",
                            Local::now().naive_local(),
                            self.id,
                            keep_alive
                        );
                        return Ok(SessionEnd::Abnormal);
                    }
                }
            } else {
                read_packet(&mut self.reader).await?
            };
            if packet.is_empty() {
                println!(
                    "[{}] client {} closed connection",
                    Local::now().naive_local(),
                    self.id
                );
                return Ok(SessionEnd::Abnormal);
            }
            let message = MqttProtocol::into_internal(packet).map_err(ClientError::from)?;
            match message {
                MqttMessage::Subscribe(subscribe) => self.handle_subscribe(subscribe).await?,
                MqttMessage::Publish(publish) => self.handle_publish(publish).await?,
                MqttMessage::PubAck(packet_identifier) => {
                    self.handle_puback(packet_identifier)?;
                }
                MqttMessage::PubRec(packet_identifier) => {
                    self.handle_pubrec(packet_identifier).await?;
                }
                MqttMessage::PubRel(packet_identifier) => {
                    self.handle_pubrel(packet_identifier).await?;
                }
                MqttMessage::PubComp(packet_identifier) => {
                    self.handle_pubcomp(packet_identifier).await?;
                }
                MqttMessage::PingReq => self.handle_pingreq()?,
                MqttMessage::Disconnect => {
                    println!(
                        "[{}] client {} disconnected",
                        Local::now().naive_local(),
                        self.id
                    );
                    return Ok(SessionEnd::Clean);
                }
                MqttMessage::Connect(_) => {
                    return Err(ClientError::Protocol(ProtocolError::InvalidPacketType(
                        0x01,
                    )));
                }
            }
        }
    }

    async fn handle_subscribe(&mut self, subscribe: SubscribePacket) -> Result<(), ClientError> {
        let mut return_codes = Vec::with_capacity(subscribe.subscriptions.len());
        for sub in &subscribe.subscriptions {
            let granted = self
                .broker
                .subscribe(&self.id, &sub.topic_filter, sub.qos)
                .await;
            return_codes.push(granted);
        }
        let suback = SubAckPacket {
            packet_identifier: subscribe.packet_identifier,
            return_codes,
        };
        self.send_response(MqttResponse::SubAck(suback))?;
        Ok(())
    }

    async fn handle_publish(&mut self, publish: PublishPacket) -> Result<(), ClientError> {
        match publish.qos {
            0 => {
                self.broker.publish(&self.id, &publish).await;
                Ok(())
            }
            1 => {
                self.broker.publish(&self.id, &publish).await;
                let packet_identifier = publish
                    .packet_identifier
                    .ok_or(ProtocolError::MissingPacketIdentifier)?;
                let puback = PubAckPacket { packet_identifier };
                self.send_response(MqttResponse::PubAck(puback))?;
                Ok(())
            }
            2 => self.handle_publish_qos2(publish).await,
            qos => Err(ClientError::from(ProtocolError::UnsupportedQoS(qos))),
        }
    }

    async fn handle_publish_qos2(&mut self, publish: PublishPacket) -> Result<(), ClientError> {
        let packet_identifier = publish
            .packet_identifier
            .ok_or(ProtocolError::MissingPacketIdentifier)?;
        match self.inbound_qos2.entry(packet_identifier) {
            Entry::Vacant(entry) => {
                entry.insert(publish);
            }
            Entry::Occupied(_) => {
                // duplicate publish; keep original payload for exactly-once delivery
            }
        }
        self.send_response(MqttResponse::PubRec(packet_identifier))?;
        Ok(())
    }

    async fn handle_pubrel(&mut self, packet_identifier: u16) -> Result<(), ClientError> {
        if let Some(publish) = self.inbound_qos2.remove(&packet_identifier) {
            self.broker.publish(&self.id, &publish).await;
        }
        self.send_response(MqttResponse::PubComp(packet_identifier))?;
        Ok(())
    }

    async fn handle_pubrec(&mut self, packet_identifier: u16) -> Result<(), ClientError> {
        let should_send_pubrel = {
            let mut pending = self.outbound_qos2.lock().await;
            if let Some(state) = pending.get_mut(&packet_identifier) {
                *state = OutboundQoS2State::AwaitingPubcomp;
                true
            } else {
                false
            }
        };

        if should_send_pubrel {
            self.send_response(MqttResponse::PubRel(packet_identifier))?;
        }
        Ok(())
    }

    async fn handle_pubcomp(&mut self, packet_identifier: u16) -> Result<(), ClientError> {
        self.outbound_qos2.lock().await.remove(&packet_identifier);
        Ok(())
    }

    fn handle_puback(&mut self, _packet_identifier: u16) -> Result<(), ClientError> {
        // Currently QoS1 acknowledgements are treated as fire-and-forget.
        Ok(())
    }

    fn handle_pingreq(&mut self) -> Result<(), ClientError> {
        self.send_response(MqttResponse::PingResp)?;
        Ok(())
    }

    fn send_response(&self, response: MqttResponse) -> Result<(), ClientError> {
        if let Some(bytes) = MqttProtocol::from_internal(response).map_err(ClientError::from)? {
            self.send_raw(bytes)?;
        }
        Ok(())
    }

    fn send_raw(&self, bytes: Vec<u8>) -> Result<(), ClientError> {
        self.outbound
            .send(ClientCommand::SendPacket(bytes))
            .map_err(|_| ClientError::ChannelClosed)
    }

    fn into_parts(
        self,
    ) -> (
        String,
        Arc<Broker>,
        mpsc::UnboundedSender<ClientCommand>,
        Option<LastWill>,
    ) {
        (self.id, self.broker, self.outbound, self.will)
    }
}

enum SessionEnd {
    Clean,
    Abnormal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutboundQoS2State {
    AwaitingPubrec,
    AwaitingPubcomp,
}

pub async fn read_packet<R>(reader: &mut R) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut first = [0u8; 1];
    if let Err(err) = reader.read_exact(&mut first).await {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            return Ok(Vec::new());
        }
        return Err(err);
    }
    let mut buffer = vec![first[0]];
    let remaining_length = read_remaining_length(reader, &mut buffer).await?;
    let mut payload = vec![0u8; remaining_length];
    reader.read_exact(&mut payload).await?;
    buffer.extend_from_slice(&payload);
    Ok(buffer)
}

async fn read_remaining_length<R>(reader: &mut R, buffer: &mut Vec<u8>) -> io::Result<usize>
where
    R: AsyncRead + Unpin,
{
    let mut multiplier = 1usize;
    let mut value = 0usize;

    for _ in 0..4 {
        let byte = reader.read_u8().await?;
        buffer.push(byte);
        value += ((byte & 0x7F) as usize) * multiplier;
        if (byte & 0x80) == 0 {
            return Ok(value);
        }
        multiplier *= 128;
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "malformed remaining length",
    ))
}

async fn write_loop(
    mut writer: WriteHalf<ClientStream>,
    mut rx: mpsc::UnboundedReceiver<ClientCommand>,
    pending_qos2: Arc<Mutex<HashMap<u16, OutboundQoS2State>>>,
) -> Result<(), io::Error> {
    let mut next_packet_id: u16 = 1;
    while let Some(cmd) = rx.recv().await {
        match cmd {
            ClientCommand::SendPacket(bytes) => {
                writer.write_all(&bytes).await?;
                writer.flush().await?;
            }
            ClientCommand::Publish {
                topic,
                payload,
                qos,
                retain,
            } => {
                let packet_identifier = if qos > 0 {
                    let id = next_packet_id;
                    next_packet_id = next_packet_id.wrapping_add(1);
                    if next_packet_id == 0 {
                        next_packet_id = 1;
                    }
                    Some(id)
                } else {
                    None
                };
                let packet = PublishPacket {
                    dup: false,
                    qos,
                    retain,
                    topic_name: topic,
                    packet_identifier,
                    payload,
                }
                .encode()
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err.to_string()))?;
                writer.write_all(&packet).await?;
                writer.flush().await?;

                if qos == 2
                    && let Some(id) = packet_identifier
                {
                    pending_qos2
                        .lock()
                        .await
                        .insert(id, OutboundQoS2State::AwaitingPubrec);
                }
            }
        }
    }
    Ok(())
}
