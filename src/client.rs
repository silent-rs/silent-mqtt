use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use chrono::Local;
use scru128::scru128_string;
use silent::BoxedConnection;
use silent::Protocol;
use thiserror::Error;
use tokio::io::split;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::sync::mpsc;

use crate::broker::Broker;
use crate::protocol::{
    ConnectPacket, MqttMessage, MqttProtocol, MqttResponse, ProtocolError, PubAckPacket,
    PublishPacket, SubAckPacket, SubscribePacket,
};

pub type ClientStream = BoxedConnection;

pub struct ClientSession {
    id: String,
    peer: SocketAddr,
    reader: ReadHalf<ClientStream>,
    outbound: mpsc::UnboundedSender<ClientCommand>,
    broker: Arc<Broker>,
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

        let writer_handle = tokio::spawn(write_loop(writer, rx));

        let mut session = ClientSession {
            id: client_id.clone(),
            peer,
            reader,
            outbound: tx.clone(),
            broker: broker.clone(),
        };

        println!(
            "[{}] client {} connected from {} (clean_session={}, keep_alive={})",
            Local::now().naive_local(),
            session.id,
            session.peer,
            connect.clean_session,
            connect.keep_alive
        );

        let read_result = session.read_loop(connect.keep_alive).await;

        let (client_id, broker, outbound) = session.into_parts();
        broker.unregister_client(&client_id).await;
        drop(outbound);

        let writer_result = match writer_handle.await? {
            Ok(()) => Ok(()),
            Err(err) => Err(ClientError::Io(err)),
        };

        writer_result?;
        read_result
    }

    async fn read_loop(&mut self, _keep_alive: u16) -> Result<(), ClientError> {
        loop {
            let packet = read_packet(&mut self.reader).await?;
            if packet.is_empty() {
                return Ok(());
            }
            let message = MqttProtocol::into_internal(packet).map_err(ClientError::from)?;
            match message {
                MqttMessage::Subscribe(subscribe) => self.handle_subscribe(subscribe).await?,
                MqttMessage::Publish(publish) => self.handle_publish(publish).await?,
                MqttMessage::PingReq => self.handle_pingreq()?,
                MqttMessage::Disconnect => {
                    println!(
                        "[{}] client {} disconnected",
                        Local::now().naive_local(),
                        self.id
                    );
                    return Ok(());
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
        self.broker.publish(&self.id, &publish).await;

        if publish.qos == 1 {
            let packet_identifier = publish
                .packet_identifier
                .ok_or(ProtocolError::MissingPacketIdentifier)?;
            let puback = PubAckPacket { packet_identifier };
            self.send_response(MqttResponse::PubAck(puback))?;
        }
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

    fn into_parts(self) -> (String, Arc<Broker>, mpsc::UnboundedSender<ClientCommand>) {
        (self.id, self.broker, self.outbound)
    }
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
            }
        }
    }
    Ok(())
}
