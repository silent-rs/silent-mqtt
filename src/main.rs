use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use chrono::Local;
use silent::BoxError;
use silent::Connection;
use silent::NetServer;
use silent::SocketAddr as SilentSocketAddr;
use tokio::io::AsyncWriteExt;

mod broker;
mod client;
// allow dead code for now
#[allow(dead_code)]
mod protocol;

use broker::Broker;
use client::{ClientError, ClientSession, read_packet};
use protocol::{ConnAckPacket, ConnectPacket, ConnectReturnCode, ProtocolError};

#[tokio::main]
async fn main() -> io::Result<()> {
    let addr: SocketAddr = "0.0.0.0:1883".parse().expect("invalid bind address");
    let server = NetServer::bind(addr).expect("failed to start listener");
    for addr in server.local_addrs() {
        println!("silent-mqtt broker listening on {}", addr);
    }
    let broker = Broker::new();

    server
        .run(move |stream, peer| {
            let broker = broker.clone();
            async move {
                handle_client(stream, peer, broker)
                    .await
                    .map_err(|err| -> BoxError { Box::new(err) })
            }
        })
        .await
}

async fn handle_client(
    mut stream: Box<dyn Connection + Send + Sync + 'static>,
    peer: SilentSocketAddr,
    broker: Arc<Broker>,
) -> Result<(), ServerError> {
    #[allow(unreachable_patterns)]
    let peer_addr = match peer {
        SilentSocketAddr::Tcp(addr) => addr,
        other => {
            return Err(ServerError::UnsupportedPeer(other.to_string()));
        }
    };
    let packet = read_packet(&mut stream).await?;
    let (connect, _) = ConnectPacket::decode(&packet)?;
    println!(
        "[{}] accepted CONNECT from {} (client_id={}, clean_session={}, keep_alive={})",
        Local::now().naive_local(),
        peer_addr,
        connect.client_id,
        connect.clean_session,
        connect.keep_alive
    );

    let connack = ConnAckPacket {
        session_present: false,
        return_code: ConnectReturnCode::Accepted,
    };
    stream.write_all(&connack.encode()).await?;
    stream.flush().await?;

    ClientSession::start(stream, peer_addr, connect, broker).await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
enum ServerError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("client error: {0}")]
    Client(#[from] ClientError),
    #[error("unsupported peer address type: {0}")]
    UnsupportedPeer(String),
}
