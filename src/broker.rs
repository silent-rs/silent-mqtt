use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use chrono::Local;
use tokio::sync::RwLock;
use tokio::sync::mpsc;

use crate::client::ClientCommand;
use crate::protocol::{LastWill, PublishPacket, SubscribeReturnCode};

#[derive(Clone)]
struct Subscriber {
    qos: u8,
    sender: mpsc::UnboundedSender<ClientCommand>,
}

#[derive(Default)]
pub struct Broker {
    clients: RwLock<HashMap<String, mpsc::UnboundedSender<ClientCommand>>>,
    subscriptions: RwLock<HashMap<String, HashMap<String, Subscriber>>>,
}

impl Broker {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn register_client(
        self: &Arc<Self>,
        client_id: String,
        sender: mpsc::UnboundedSender<ClientCommand>,
    ) {
        self.clients.write().await.insert(client_id, sender);
    }

    pub async fn unregister_client(self: &Arc<Self>, client_id: &str) {
        self.clients.write().await.remove(client_id);
        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.retain(|_, subs| {
            subs.remove(client_id);
            !subs.is_empty()
        });
    }

    pub async fn subscribe(
        self: &Arc<Self>,
        client_id: &str,
        topic_filter: &str,
        requested_qos: u8,
    ) -> SubscribeReturnCode {
        let sender = {
            let clients = self.clients.read().await;
            clients.get(client_id).cloned()
        };
        let sender = match sender {
            Some(sender) => sender,
            None => return SubscribeReturnCode::Failure,
        };

        let granted_qos = requested_qos.min(2);
        let return_code = match granted_qos {
            0 => SubscribeReturnCode::SuccessQoS0,
            1 => SubscribeReturnCode::SuccessQoS1,
            _ => SubscribeReturnCode::SuccessQoS2,
        };

        let mut subscriptions = self.subscriptions.write().await;
        let entry = subscriptions
            .entry(topic_filter.to_string())
            .or_insert_with(HashMap::new);
        entry.insert(
            client_id.to_string(),
            Subscriber {
                qos: granted_qos,
                sender,
            },
        );

        return_code
    }

    pub async fn publish(self: &Arc<Self>, publisher_id: &str, packet: &PublishPacket) {
        let recipients: Vec<(String, u8, mpsc::UnboundedSender<ClientCommand>)> = {
            let subscriptions = self.subscriptions.read().await;
            subscriptions
                .get(&packet.topic_name)
                .map(|subs| {
                    subs.iter()
                        .map(|(client_id, sub)| (client_id.clone(), sub.qos, sub.sender.clone()))
                        .collect()
                })
                .unwrap_or_default()
        };

        for (client_id, qos, sender) in recipients {
            let deliver_qos = packet.qos.min(qos);
            if let Err(err) = sender.send(ClientCommand::Publish {
                topic: packet.topic_name.clone(),
                payload: packet.payload.clone(),
                qos: deliver_qos,
                retain: packet.retain,
            }) {
                eprintln!(
                    "[{}] failed to deliver message to subscriber {}: {}. removing subscription",
                    Local::now().naive_local(),
                    client_id,
                    err
                );
                let _ = self.unregister_client(&client_id).await;
            }
        }

        // Optionally log the publish for observability.
        println!(
            "[{}] {} published message on topic '{}' (payload {} bytes, qos={})",
            Local::now().naive_local(),
            publisher_id,
            packet.topic_name,
            packet.payload.len(),
            packet.qos
        );
    }

    pub async fn publish_will(self: &Arc<Self>, will: &LastWill) {
        let packet = PublishPacket {
            dup: false,
            qos: will.qos,
            retain: will.retain,
            topic_name: will.topic.clone(),
            packet_identifier: None,
            payload: Bytes::from(will.message.clone()),
        };
        self.publish("$will", &packet).await;
    }
}
